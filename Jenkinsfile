@Library('jenkins-helpers') _

def label = "cognite-replicator-${UUID.randomUUID().toString()}"
def imageName = "cognite/cognite-replicator"

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
        containerTemplate(name: 'gitleaks',
            command: '/bin/cat -',
            image: 'eu.gcr.io/cognitedata/gitleaks:latest',
            resourceRequestCpu: '300m',
            resourceRequestMemory: '500Mi',
            resourceLimitCpu: '1',
            resourceLimitMemory: '1Gi',
            ttyEnabled: true)
    ],
    volumes: [
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
                scmVars = checkout(scm)
                gitCommit = scmVars.GIT_COMMIT
            }
        }

        container('gitleaks') {
            stage('Gitleaks scan for secrets') {
                sh('gitleaks --repo-path=`pwd` --verbose --redact')
            }
        }

        container('python') {
            stage('Install dependencies') {
                sh('pip3 install poetry')
                sh('pip3 install twine')
                sh('poetry config settings.virtualenvs.create false')
                sh('poetry install')
            }
            stage('Validate code') {
                sh("black --check .")
                sh("isort --check-only -rc .")
            }
            stage('Build') {
                sh("poetry build")
            }
            stage('Run tests') {
                sh("pyenv local 3.6.6 3.7.4 3.8.0")
                sh("tox")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
                sh('bash </codecov-script/upload-report.sh')
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build docs') {
                dir('./docs'){
                    sh("sphinx-build -W -b html ./source ./build")
                }
            }
            if (env.BRANCH_NAME == 'master') {
                stage('Release to PyPI') {
                    sh("twine upload --verbose --config-file /pypi/.pypirc dist/* || echo 'version exists'")
                }
            }
        }

        container('docker') {
            stage('Build docker image') {
                sh("docker build -t ${imageName}:${gitCommit} .")
                sh('cat /dockerhub-credentials/DOCKER_PASSWORD | docker login -u "$(cat /dockerhub-credentials/DOCKER_USERNAME)" --password-stdin')
            }
            if (env.CHANGE_ID) {
                stage("Publish PR image") {
                    def prImage = "${imageName}-dev:pr-${env.CHANGE_ID}"
                    sh("docker tag ${imageName}:${gitCommit} ${prImage}")
                    sh("docker push ${prImage}")
                    pullRequest.comment("[pr-bot]\nRun this build with `docker run --rm -it ${prImage}`")
                }
            } else if (env.BRANCH_NAME == 'master') {
                stage('Push to GCR') {
                    def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/replicator/_version.py | cut -d\\" -f2').trim()
                    sh("docker tag ${imageName}:${gitCommit} ${imageName}:latest")
                    sh("docker tag ${imageName}:${gitCommit} ${imageName}:${currentVersion}")
                    sh("docker push ${imageName}:${currentVersion}")
                    sh("docker push ${imageName}:latest")
                }
            }
        }
    }
}
