import redis
import json

class RepoDAO(object):
    def __init__(self):
        self.redis = redis.StrictRedis(host='redis', port=6379, db=5, decode_responses=True)
        
    def save(self, repo_id, repo_data):
        """
        Github 레파지토리 정보를 Redis에 저장하는 함수

        :param repo_id: 저장할 레파지토리의 ID
        :param repo_data: 저장할 레파지토리 정보 데이터
        """
        self.redis.hset("repo", repo_id, json.dumps(repo_data))

    def get(self, repo_id):
        """
        Redis에서 레파지토리 정보를 조회하는 함수

        :param repo_id: 조회할 레파지토리의 ID
        :return: 레파지토리 정보 데이터 (없을 경우 None)
        """
        repo_data = self.redis.hget("repo", repo_id)
        if repo_data:
            return json.loads(repo_data)
        else:
            return None

    def delete(self, repo_id):
        """
        Redis에서 레파지토리 정보를 삭제하는 함수

        :param repo_id: 삭제할 레파지토리의 ID
        :return: 삭제 성공 여부 (True: 삭제됨, False: 삭제되지 않음)
        """
        if self.redis.hexists("repo", repo_id):
            self.redis.hdel("repo", repo_id)
            return True
        else:
            return False

    def update(self, repo_id, repo_data):
        """
        Redis에서 레파지토리 정보를 업데이트하는 함수

        :param repo_id: 업데이트할 레파지토리의 ID
        :param repo_data: 업데이트할 레파지토리 정보 데이터
        :return: 업데이트 성공 여부 (True: 업데이트됨, False: 업데이트되지 않음)
        """
        if self.redis.hexists("repo", repo_id):
            self.redis.hset("repo", repo_id, json.dumps(repo_data))
            return True
        else:
            return False
    
    def get_all_repo_ids(self):
        """
        Redis에서 모든 레파지토리의 ID를 조회하는 함수

        :return: 레파지토리 ID 리스트
        """
        repo_ids = self.redis.hkeys("repo")
        return repo_ids

    def get_all_repo_data(self):
        """
        Redis에서 모든 레파지토리 데이터를 조회하는 함수

        :return: 모든 레파지토리 데이터 (레파지토리 ID를 키로 하는 딕셔너리)
        """
        all_repo_data = {}
        repo_ids = self.get_all_repo_ids()
        for repo_id in repo_ids:
            repo_data = self.get(repo_id)
            if repo_data:
                all_repo_data[repo_id] = repo_data
        return all_repo_data