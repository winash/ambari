"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management import *
from setup_logfeeder import setup_logfeeder

class LogFeeder(Script):

  def install(self, env):
    import params
    env.set_params(params)
    
    self.install_packages(env)
    
  def configure(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    
    setup_logfeeder()

  def start(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    self.configure(env)

    Execute(format("{logfeeder_dir}/run.sh"),
            environment = {'LOGFEEDER_INCLUDE': format('{logsearch_logfeeder_conf}/logfeeder-env.sh')},
            user=params.logfeeder_user
            )

  def stop(self, env, upgrade_type=None):
    import params
    env.set_params(params)

    Execute (format("kill `cat {logfeeder_pid_file}`"), 
             user=params.logfeeder_user , 
             only_if = format("test -f {logfeeder_pid_file}")
    )
    File(params.logfeeder_pid_file, 
         action="delete"
    )
      	
  def status(self, env):
    import status_params
    env.set_params(status_params)  
    
    check_process_status(status_params.logfeeder_pid_file)


if __name__ == "__main__":
  LogFeeder().execute()
