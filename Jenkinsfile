pipeline {
  agent any
  stages {
    stage('Prepare') {
      steps {
        sh 'mkdir -p arivi-core'
        dir(path: 'arivi-core') {
          git(url: 'https://github.com/xoken/arivi-core/', branch: 'master')
        }

        sh 'mkdir -p xoken-core'
        dir(path: 'xoken-core') {
          git(url: 'https://github.com/xoken/xoken-core/', branch: 'master')
        }

        sh 'mkdir -p xoken-node'
        dir(path: 'xoken-node') {
          git(url: 'https://github.com/xoken/xoken-node/', branch: "${env.BRANCH_NAME}")
        }

      }
    }

    stage('Clean') {
      steps {
        dir(path: 'xoken-node') {
          sh 'stack clean'
        }

      }
    }

    stage('Build') {
      steps {
        dir(path: 'xoken-node') {
          sh 'stack install  --local-bin-path  ../build/reg/'
          sh 'stack install  --executable-profiling  --local-bin-path  ../build/prof/'
        }

        archiveArtifacts(artifacts: 'build/**/xoken-nexa', followSymlinks: true)
      }
    }

    stage('Release') {
      steps {
       echo 'Starting docker containers'
       dir(path: 'xoken-node'){
              sh 'rm -f /tmp/ubuntu1804.cid'
              sh 'docker run -t -d --cidfile /tmp/ubuntu1804.cid -w  /opt/work/xoken-node  xoken-nexa/ubuntu18.04 sh'
              sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu1804.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu1804.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) stack clean'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) stack install  --local-bin-path  . '
              sh 'docker cp $(cat /tmp/ubuntu1804.cid):/opt/work/xoken-node/xoken-nexa  . '
              sh 'rm -f /tmp/ubuntu1804.cid'
              sh 'sha256sum /opt/work/xoken-node/xoken-nexa > Checksum_SHA256'
              sh 'zip xoken-nexa_"$(basename $(git symbolic-ref HEAD))"_ubuntu1804.zip ./xoken-nexa ReleaseNotes README Checksum_SHA256 '
            }
       dir(path: 'xoken-node'){
              sh 'rm -f /tmp/ubuntu2004.cid'
              sh 'docker run -t -d --cidfile /tmp/ubuntu2004.cid -w  /opt/work/xoken-node  xoken-nexa/ubuntu18.04 sh'
              sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu2004.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu2004.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) git pull'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) stack clean'
              sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) stack install  --local-bin-path  . '
              sh 'docker cp $(cat /tmp/ubuntu2004.cid):/opt/work/xoken-node/xoken-nexa  . '
              sh 'rm -f /tmp/ubuntu2004.cid'
              sh 'sha256sum /opt/work/xoken-node/xoken-nexa > Checksum_SHA256'
              sh 'zip xoken-nexa_"$(basename $(git symbolic-ref HEAD))"_ubuntu2004.zip ./xoken-nexa ReleaseNotes README Checksum_SHA256 '
            }
            archiveArtifacts(artifacts: 'xoken-node/xoken-nexa*.zip', followSymlinks: true)
        }
       }  
  }
  
      post {
          unsuccessful {
                  emailext(subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', body: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS    ||   Please check attached logfile for more details.', attachLog: true, from: 'buildmaster@xoken.org', replyTo: 'buildmaster@xoken.org', to: 'jenkins-notifications@xoken.org')
           
          }
          fixed {
                  emailext(subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', body: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS  ||  Previous build was not successful and the current builds status is SUCCESS ||  Please check attached logfile for more details.', attachLog: true, from: 'buildmaster@xoken.org', replyTo: 'buildmaster@xoken.org', to: 'jenkins-notifications@xoken.org')
          }
      }
  
  
}
