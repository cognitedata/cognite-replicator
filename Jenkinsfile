@Library('jenkins-helpers') _

def label = "cognite-replicator-${UUID.randomUUID().toString()}"
def imageName = "cognite/cognite-replicator"
def devImageName = "cognite/cognite-replicator-dev"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:2019-10-18T1122-3e874f7',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            ttyEnabled: true),
        containerTemplate(name: 'docker',
            command: '/bin/cat -',
            image: 'docker:18.06.1-ce',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '1000Mi',
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        secretVolume(secretName: 'cognitecicd-dockerhub', mountPath: '/dockerhub-credentials'),
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'cognite-replicator'),
        envVar(key: 'CI', value: '1'),
        // /codecov-script/upload-report.sh relies on the following
        // Jenkins and Github environment variables.
        envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
        envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
        envVar(key: 'BUILD_URL', value: env.BUILD_URL),
        envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
    ]) {
    node(label) {
        def gitCommit
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                gitCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
            }
        }

        container('python') {
            stage('Install poetry') {
                sh('pip3 install poetry')
            }
            stage('Install all dependencies') {
                sh('pip3 install twine')
                sh('poetry config settings.virtualenvs.create false')
                sh('poetry install')
            }
            stage('Check code') {
                sh("pyenv local 3.6.6 3.7.4 3.8.0")
                sh("tox -p auto")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
            stage('Validate code format') {
                sh("black --check .")
                sh("isort --check-only -rc .")
            }

            stage('Build') {
                sh("poetry build")
            }
            stage('Build Docs') {
                dir('./docs'){
                    sh("poetry run sphinx-build -W -b html ./source ./build")
                }
            }
            stage('Upload coverage reports') {
                sh('bash </codecov-script/upload-report.sh')
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }

             if (env.BRANCH_NAME == 'master') {
                 stage('Release to pypi') {
                     sh("twine upload --verbose --config-file /pypi/.pypirc dist/* || echo 'version exists'")
                 }
             }
        }

        container('docker') {
            stage('Build docker image') {
                sh("docker build -t ${imageName}:${gitCommit} .")
            }

            stage('Login to docker hub') {
                sh('cat /dockerhub-credentials/DOCKER_PASSWORD | docker login -u "$(cat /dockerhub-credentials/DOCKER_USERNAME)" --password-stdin')
            }

            if (env.CHANGE_ID) {
                stage("Publish PR image") {
                    def prImage = "${devImageName}:pr-${env.CHANGE_ID}"
                    sh("docker tag ${imageName}:${gitCommit} ${prImage}")
                    sh("docker push ${prImage}")
                    pullRequest.comment("[pr-bot]\nRun this build with `docker run --rm -it ${prImage}`")
                }
            } else if (env.BRANCH_NAME == 'master') {
                stage('Push to GCR') {
                    sh("docker tag ${imageName}:${gitCommit} ${imageName}:latest")
                    sh("docker push ${imageName}:${gitCommit}")
                    sh("docker push ${imageName}:latest")
                }
            }
        }
    }
}
