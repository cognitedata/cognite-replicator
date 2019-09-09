@Library('jenkins-helpers@v0.1.12') _

def label = "cognite-replicator-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:7040fac',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'cognite-sdk-python'),
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
                sh("pip3 install poetry")
            }
            stage('Install all dependencies') {
                sh("poetry install")
            }
            stage('Test code') {
                sh("poetry run pytest --cov cognite")
            }
            stage('Upload report to codecov.io') {
                sh('bash </codecov-script/upload-report.sh')
            }
            stage('Check code') {
                sh("poetry run black -l 120 --check .")
            }
            stage('Build') {
                sh("poetry build")
            }

            def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/replicator/__init__.py | cut -d\\" -f2').trim()
            println("This version: " + currentVersion)
//             if (env.BRANCH_NAME == 'master') {
//                 stage('Release') {
//                     sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
//                 }
//             }
        }
    }
}
