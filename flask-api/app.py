from flask import Flask
from flask_restx import Api
from tokens.routes import tokens_bp, tokens_ns
from tokens.util import get_aws_secret, update_tokens_usage
from repo.routes import repo_ns, repo_bp
import os

app = Flask(__name__)

api = Api(
    app,
    version='0.1',
    title="Redis API Server",
    description="Redis API Server for Airflow DAG",
    terms_url="/",
    contact="hmk9667@gmail.com",
)

api.add_namespace(tokens_ns, '/tokens')
api.add_namespace(repo_ns, '/repo')
app.register_blueprint(tokens_bp, url_prefix='/tokens')
app.register_blueprint(repo_bp, url_prefix='/repo')


@app.before_first_request
def execute_once():
    """
    # 초기 1회 셋팅 (서비스 재시작시 가동)
    """
    TOKEN_NAME = os.environ.get("TOKEN_NAME")
    git_tokens = get_aws_secret(TOKEN_NAME)
    update_tokens_usage(git_tokens, is_update=False)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)