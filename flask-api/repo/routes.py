from flask import Blueprint, request
from flask_restx import Api, Resource, Namespace, fields 
from .util import RepoDAO

repo_bp = Blueprint('repo', __name__)
api = Api(repo_bp, version='0.1', title="REDIS CRUD API for managing Github Repository")
repo_ns = Namespace('repo', description='Git Repo CRUD')

repo_model = repo_ns.model('Repository', {
    'ID': fields.String(required=True, description='Repository ID'),
    'FULL_NM': fields.String(required=True, description='Full Repository Name'),
})

DAO = RepoDAO()

class Repo(Resource):

    @repo_ns.route('/save_repo')
    class SaveRepo(Resource):

        @repo_ns.doc('save_repo')
        @repo_ns.expect(repo_model) 
        def post(self):
            """
            # Repo 정보 저장 (Post)

            :return: 저장 결과 메세지
            """
            try:
                repo_data = request.json
                repo_id = repo_data['ID']
                DAO.save(repo_id, repo_data)
                return {"message": f"Success to save Repository {repo_id} to Redis."}, 201
            except Exception as e:
                return {"error": str(e)}, 500
            
    @repo_ns.route('/<int:repo_id>')
    class RepoById(Resource):

        @repo_ns.doc('get_repo')
        def get(self, repo_id):
            """
            # Repo 정보 조회 (Get)
            
            :param repo_id: 조회할 레포지토리의 ID
            :return: 레포지토리 정보 또는 에러 메세지
            """
            repo_data = DAO.get(repo_id)
            if repo_data:
                return repo_data, 200
            else:
                return {"message": f"Repository {repo_id} not found."}, 404

        @repo_ns.doc('delete_repo')
        def delete(self, repo_id):
            """
            # Repo 정보 삭제 (Delete)

            :param repo_id: 삭제할 레포지토리의 ID
            :return: 삭제 결과 메세지 또는 에러 메세지
            """
            if DAO.delete(repo_id):
                return {"message": f"Success to delete Repository {repo_id} from Redis."}, 200
            else:
                return {"message": f"Repository {repo_id} not found."}, 404

    @repo_ns.route('/update_repo/<int:repo_id>')
    class UpdateRepo(Resource):
        
        @repo_ns.doc('update_repo')
        @repo_ns.expect(repo_model)  
        def put(self, repo_id):
            """
            # Repo 정보 업데이트 (Put)
            
            :param repo_id: 업데이트할 레포지토리의 ID
            :return: 업데이트 결과 메세지 또는 에러 메세지
            """
            try:
                repo_data = request.json
                if DAO.update(repo_id, repo_data):
                    return {"message": f"Success to update Repository {repo_id} in Redis."}, 200
                else:
                    return {"message": f"Repository {repo_id} not found."}, 404
            except Exception as e:
                return {"error": str(e)}, 500


api.add_namespace(repo_ns, path='/repo')



