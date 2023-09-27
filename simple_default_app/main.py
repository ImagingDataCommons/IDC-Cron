"""

Copyright 2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

#
# To *just* run cron in App Engine Standard (e.g. admin project), we need to first deploy
# the default service. Yeah, we could call cron "default" but this lets us stick with using the
# "cron" service. The default service just always return 404.
#

from flask import Flask, abort

app = Flask(__name__)

@app.route('/')
def hello():
    abort(404)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
