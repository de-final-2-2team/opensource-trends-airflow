from flask import Blueprint, jsonify
from flask_restx import Api, Resource, Namespace
from .util import *


tokens_bp = Blueprint('tokens', __name__)
api = Api(tokens_bp, version='0.1', title="Tokens API")
tokens_ns = Namespace('token')

@tokens_ns.route('/get_all_tokens')
class AllTokens(Resource):
    def get(self):
        """
        # 모든 토큰의 정보를 가져옴
        
        :return: 토큰 정보 딕셔너리
        """
        all_tokens = {}
        keys = redis.keys("token*")
        for key in keys:
            token_info = redis.hgetall(key)
            all_tokens[key] = token_info
        return jsonify(all_tokens)

@tokens_ns.route('/get_token_info/<string:token_key>')
class TokenInfo(Resource):
    def get(self, token_key:str):
        """
        # 특정 토큰의 정보를 가져옴
        
        :param token_key: 토큰의 키
        :return: 토큰 정보 딕셔너리
        """
        token_info = redis.hgetall(token_key)
        return jsonify(token_info) 

@tokens_ns.route('/get_available_token/<string:api_type>')
class AvailableToken(Resource):
    def get(self, api_type: str):
        """
        # 사용 가능한 토큰 가져오기

        :param api_type: 가져올 토큰 타입 ('total', 'core', 'search')
        """
        logging.debug(f"api_type :{api_type}")
        if api_type not in ("total", "core", "search"):
            return jsonify("Please enter a valid <api_type>. <api_type> should be 'total', 'core', or 'search'")

        tokens = []
        for key in redis.keys("*"):
            token_info = redis.hgetall(key)
            allocated = token_info.get('allocated')
            remaining = int(token_info.get(f"{api_type}_remaining",0))
            reset = int(token_info.get(f"{api_type}_reset",0))
            print(f"{key} 확인 : {allocated} {remaining} {reset}")
            if allocated == "false" and remaining > 0:
                tokens.append({
                    "key": key,
                    "remaining": remaining,
                    "reset": reset
                })
        
        if tokens:
            tokens = sorted(tokens, key=lambda x: (x["reset"], -x["remaining"]))
            logging.debug(tokens)
            token_info = tokens[0]
            redis.hset(token_info["key"], "allocated", "true")
            token_all_info = redis.hgetall(token_info['key'])
            return jsonify(token_all_info)
        else:
            return jsonify(message="No available tokens")

@tokens_ns.route('/return_token/<string:token_key>')
class ReturnToken(Resource):
    def get(self, token_key: str):
        """
        # 토큰 반납하기

        :param token_key: 반납할 토큰의 키
        """
        is_allocated = redis.hget(token_key, "allocated")
        if is_allocated == "false":
            return jsonify("Not Access. You did not have permission to return token")
        redis.hset(token_key, "allocated", "false")
        return jsonify("Succees to Return token")

@tokens_ns.route('/delete_all_tokens')
class DeleteToken(Resource):
    def post(self):
        """
        # 모든 토큰 삭제하기
        """
        keys = redis.keys("*")
        for key in keys:
            redis.delete(key)
        return jsonify("All tokens have been deleted.")

@tokens_ns.route('/init_tokens')
class InitTokens(Resource):
    def get(self):
        """
        # 모든 토큰 셋팅
        """
        TOKEN_NAME = os.environ.get("TOKEN_NAME")
        git_tokens = get_aws_secret(TOKEN_NAME)
        update_tokens_usage(git_tokens, is_update=True)

# 네임스페이스 라우트 등록
api.add_resource(AllTokens, '/get_all_tokens')
api.add_resource(TokenInfo, '/get_token_info/<string:token_key>')
api.add_resource(AvailableToken, '/get_available_token/<string:api_type>')
api.add_resource(ReturnToken, '/return_token/<string:token_key>')
api.add_resource(DeleteToken, '/delete_all_tokens')
api.add_resource(InitTokens, '/init_tokens')

# 초기화 코드
@tokens_bp.before_request
def init_tokens():
    # AWS Secret Manager에서 토큰 정보 가져와 redis에 업데이트
    TOKEN_NAME = os.environ.get("TOKEN_NAME")
    git_tokens = get_aws_secret(TOKEN_NAME)
    update_tokens_usage(git_tokens, is_update=True)