#!/usr/bin/env groovy

@Library('workflowlibs@master') _

pipeline {
    agent { label 'spark' }

    stages {
        stage('Checkout Global Library') {
            steps {
                script{
                    globalBootstrap {
                        libraryName = "datio-workflowlibs"
                        libraryBranch = "master"

                        entrypointParams = [
                            projectType: "DEV",
                            branchExecForced: true
                        ]
                    }
                }
            }
        }
    }

    post {
        always {
            echo "We have been through the entire pipeline"
        }
        changed {
            echo "There have been some changes from the last build"
        }
        success {
            echo "Build successful"
        }
        failure {
            echo "There have been some errors"
        }
        unstable {
            echo "Unstable"
        }
        aborted {
            echo "Aborted"
        }
    }
}
