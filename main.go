package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	coreapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	var cronJobName, namespace string
	flag.StringVar(&cronJobName, "j", "", "Cron job name (required).")
	flag.StringVar(&namespace, "n", "", "Kubernetes namespace (required).")
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	f, err := os.OpenFile(cronJobName+"_pod_statuses", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var activeJob string

GetJobsLoop:
	for {
		cronJob, err := client.BatchV1().CronJobs(namespace).Get(ctx, cronJobName, metav1.GetOptions{})
		if err != nil {
			panic(err)
		}
		numActive := len(cronJob.Status.Active)

		if numActive == 0 {
			time.Sleep(5 * time.Second)
			continue GetJobsLoop
		}
		if numActive != 1 {
			panic(fmt.Errorf("expected only 1 active cron job at a time, got %d", numActive))
		}
		if newActiveJob := cronJob.Status.Active[0].Name; newActiveJob != activeJob {
			// Never seen this job before.
			// Start a goroutine that will monitor this job.
			// TODO: wait for goroutines to finish if the program gets killed.
			activeJob = newActiveJob
			res, err := monitor(ctx, client, namespace, activeJob)
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(f, "%s=%s\n", res.name, res.phase)
			log.Printf("wrote pod status for pod %s (%s)", res.name, res.phase)
		}
		time.Sleep(5 * time.Second)
	}
}

type podStatus struct {
	name  string
	phase coreapi.PodPhase
}

func monitor(ctx context.Context, client *kubernetes.Clientset, namespace, jobName string) (podStatus, error) {
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return podStatus{}, fmt.Errorf("getting job: %w", err)
	}
	var labelSelector, sep string

	for k, v := range job.Labels {
		labelSelector += sep + k + "=" + v
		sep = ","
	}
	var (
		started bool
		pods    = client.CoreV1().Pods(namespace)
	)
	for {
		select {
		case <-ctx.Done():
		default:
			podList, err := pods.List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})
			if err != nil {
				return podStatus{}, fmt.Errorf("listing pods: %w", err)
			}
			for _, pod := range podList.Items {
				if !started && pod.Status.Phase == coreapi.PodRunning {
					started = true

					log.Printf("pod %s has started running, tailing the logs", pod.Name)

					// Start tailing the logs.
					stream, err := pods.GetLogs(pod.Name, &coreapi.PodLogOptions{Follow: true}).Stream(ctx)
					if err != nil {
						return podStatus{}, fmt.Errorf("getting log stream: %w", err)
					}
					filename := pod.Name + ".logs"
					f, err := os.Create(filename)
					if err != nil {
						return podStatus{}, fmt.Errorf("opening file: %w", err)
					}
					defer f.Close()

					if _, err := io.Copy(f, stream); err != nil {
						return podStatus{}, fmt.Errorf("streaming logs: %w", err)
					}
					final, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
					if err != nil {
						return podStatus{}, fmt.Errorf("getting pod: %w", err)
					}
					log.Printf("pod %s finished with status %s", pod.Name, final.Status.Phase)
					log.Printf("monitor for %s finished", pod.Name)

					return podStatus{name: pod.Name, phase: final.Status.Phase}, nil
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
