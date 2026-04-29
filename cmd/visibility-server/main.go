package main

import (
	"context"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/config"
	"sigs.k8s.io/kueue/pkg/controller/core"
	_ "sigs.k8s.io/kueue/pkg/controller/jobs"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/tlsconfig"
	"sigs.k8s.io/kueue/pkg/visibility"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueue.AddToScheme(scheme))
	utilruntime.Must(configapi.AddToScheme(scheme))
}

type NoOpReconciler struct{}

func (r *NoOpReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func main() {
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)

	var configFile string
	flag.StringVar(&configFile, "config", "", "The path to the configuration file.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog := ctrl.Log.WithName("setup")

	// Load configuration
	options, cfg, err := config.Load(scheme, configFile)
	if err != nil {
		setupLog.Error(err, "Unable to load configuration")
		os.Exit(1)
	}
	options.LeaderElection = false

	// Set feature gates from config
	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(cfg.FeatureGates); err != nil {
		setupLog.Error(err, "Unable to set flag gates for known features")
		os.Exit(1)
	}

	// Check feature gate
	if !features.Enabled(features.StandaloneVisibilityServer) {
		setupLog.Error(nil, "StandaloneVisibilityServer feature gate is not enabled")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		setupLog.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	cCache := schdcache.New(mgr.GetClient())
	requeuer := qcache.NewRequeuer()
	if err := mgr.Add(requeuer); err != nil {
		setupLog.Error(err, "Unable to add workloadRequeuer to manager")
		os.Exit(1)
	}
	queues := qcache.NewManager(mgr.GetClient(), cCache, requeuer)

	// Setup controllers with NoOpReconciler
	// ClusterQueue
	cqRec := core.NewClusterQueueReconciler(mgr.GetClient(), queues, cCache)
	if err := builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("clusterqueue_visibility_populator").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.ClusterQueue{},
			&handler.TypedEnqueueRequestForObject[*kueue.ClusterQueue]{},
			cqRec,
		)).
		Complete(&NoOpReconciler{}); err != nil {
		setupLog.Error(err, "Unable to setup ClusterQueue controller")
		os.Exit(1)
	}

	// LocalQueue
	lqRec := core.NewLocalQueueReconciler(mgr.GetClient(), queues, cCache)
	if err := builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("localqueue_visibility_populator").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.LocalQueue{},
			&handler.TypedEnqueueRequestForObject[*kueue.LocalQueue]{},
			lqRec,
		)).
		Complete(&NoOpReconciler{}); err != nil {
		setupLog.Error(err, "Unable to setup LocalQueue controller")
		os.Exit(1)
	}

	// Workload
	wlRec := core.NewWorkloadReconciler(mgr.GetClient(), queues, cCache, mgr.GetEventRecorderFor("workload-visibility-populator"))
	if err := builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("workload_visibility_populator").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.Workload{},
			&handler.TypedEnqueueRequestForObject[*kueue.Workload]{},
			wlRec,
		)).
		Complete(&NoOpReconciler{}); err != nil {
		setupLog.Error(err, "Unable to setup Workload controller")
		os.Exit(1)
	}

	parsedTLSConfig := &tlsconfig.TLS{}
	if features.Enabled(features.TLSOptions) {
		var err error
		parsedTLSConfig, err = tlsconfig.ParseTLSOptions(cfg.TLS)
		if err != nil {
			setupLog.Error(err, "Unable to parse TLS options from configuration")
			os.Exit(1)
		}
	}

	// Start visibility server
	if features.Enabled(features.VisibilityOnDemand) {
		go func() {
			kubeConfig := ctrl.GetConfigOrDie()
			port := configapi.DefaultVisibilityBindPort
			if err := visibility.CreateAndStartVisibilityServer(context.Background(), queues, &cfg, kubeConfig, port, parsedTLSConfig); err != nil {
				setupLog.Error(err, "Unable to create and start visibility server")
				os.Exit(1)
			}
		}()
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Could not run manager")
		os.Exit(1)
	}
}
