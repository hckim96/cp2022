#!/bin/bash

# tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='workloads' -czf submission.tar.gz *
tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='workloads' --exclude='excludeMyRunTestHarness.sh' --exclude='driverInput.txt' --exclude='myTest.sh' --exclude='myPackage.sh' -czf submission.tar.gz *
