package main

import (
	"fmt"
	"os"
)

func main() {
	printUsage := func() {
		fmt.Print("Usage of dask-gateway-proxy <command>...\n\n")
		fmt.Print("Commands:\n")
		fmt.Print("  scheduler      Start the scheduler proxy\n")
		fmt.Print("  web            Start the web proxy\n")
	}

	if len(os.Args) == 1 {
		printUsage()
		os.Exit(2)
	} else {
		switch os.Args[1] {
		case "scheduler":
			schedulerProxyMain(os.Args[2:])
		case "web":
			webProxyMain(os.Args[2:])
		case "-h", "-help":
			printUsage()
		default:
			fmt.Printf("%q is not a valid command.\n", os.Args[1])
			os.Exit(2)
		}
	}
}
