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

        sh 'mkdir -p xcql'
        dir(path: 'xcql') {
          git(url: 'https://github.com/xoken/xcql/', branch: 'master')
        }

        sh 'mkdir -p cql-ffi'
        dir(path: 'cql-ffi') {
          git(url: 'https://github.com/xoken/cql-ffi/', branch: 'master')
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
        }

        archiveArtifacts(artifacts: 'build/**/xoken-nexa', followSymlinks: true)
      }
    }



      stage('Release') {
        

        steps {
          script {
            if ((env.BRANCH_NAME).startsWith("release")) {   
              echo '****** Starting Ubuntu18.04 container ******'
              dir(path: 'xoken-node'){
                      sh 'rm -f /tmp/ubuntu1804.cid'
                      sh 'docker run -t -d --cidfile /tmp/ubuntu1804.cid -w  /opt/work/xoken-node  xoken-nexa/ubuntu18.04 sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/xcql $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/cql-ffi $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) git fetch '
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) stack clean'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu1804.cid) stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/ubuntu1804.cid):/opt/work/xoken-node/xoken-nexa  . '
                      sh 'rm -f /tmp/ubuntu1804.cid'
                      sh 'sha256sum ./xoken-nexa > Checksum_SHA256'
                      sh 'zip xoken-nexa_"$(basename $(git symbolic-ref HEAD))"_ubuntu1804.zip ./xoken-nexa node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql neo4j.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
              echo '****** Starting Ubuntu20.04 container ******'
              dir(path: 'xoken-node'){
                      sh 'rm -f /tmp/ubuntu2004.cid'
                      sh 'docker run -t -d --cidfile /tmp/ubuntu2004.cid -w  /opt/work/xoken-node  xoken-nexa/ubuntu20.04 sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/xcql $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/cql-ffi $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) git fetch '
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) stack clean'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/ubuntu2004.cid) stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/ubuntu2004.cid):/opt/work/xoken-node/xoken-nexa  . '
                      sh 'rm -f /tmp/ubuntu2004.cid'
                      sh 'sha256sum ./xoken-nexa > Checksum_SHA256'
                      sh 'zip xoken-nexa_"$(basename $(git symbolic-ref HEAD))"_ubuntu2004.zip ./xoken-nexa node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql neo4j.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
              echo '****** Starting Arch Linux container ******'
              dir(path: 'xoken-node'){
                      sh 'rm -f /tmp/archlinux.cid'
                      sh 'docker run -t -d --cidfile /tmp/archlinux.cid -w  /opt/work/xoken-node  xoken-nexa/archlinux sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/xcql $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/cql-ffi $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/archlinux.cid) git fetch '
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/archlinux.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/archlinux.cid) stack clean'
                      sh 'docker exec -w /opt/work/xoken-node $(cat /tmp/archlinux.cid) env LD_PRELOAD=/usr/lib/libjemalloc.so.2 stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/archlinux.cid):/opt/work/xoken-node/xoken-nexa  . '
                      sh 'rm -f /tmp/archlinux.cid'
                      sh 'sha256sum ./xoken-nexa > Checksum_SHA256'
                      sh 'zip xoken-nexa_"$(basename $(git symbolic-ref HEAD))"_archlinux.zip ./xoken-nexa node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql neo4j.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
                    archiveArtifacts(artifacts: 'xoken-node/xoken-nexa*.zip', followSymlinks: true)
          } else { 
          echo 'skipping Docker release packaging..'
          }
        }
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
