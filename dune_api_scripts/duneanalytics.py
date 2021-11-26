# Code mostly copied from: https://github.com/itzmestar/duneanalytics

"""This provides the DuneAnalytics class implementation"""

from requests import Session

# --------- Constants --------- #

BASE_URL = "https://dune.xyz"
GRAPH_URL = 'https://core-hsr.duneanalytics.com/v1/graphql'


# --------- Constants --------- #


class DuneAnalytics:
    """
    DuneAnalytics class to act as python client for duneanalytics.com.
    All requests to be made through this class.
    """

    def __init__(self, username, password):
        """
        Initialize the object
        :param username: username for duneanalytics.com
        :param password: password for duneanalytics.com
        """
        self.csrf = None
        self.auth_refresh = None
        self.token = None
        self.username = username
        self.password = password
        self.session = Session()
        headers = {
            'origin': BASE_URL,
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'dnt': '1',
        }
        self.session.headers.update(headers)

    def login(self):
        """
        Try to login to duneanalytics.com & get the token
        :return:
        """
        login_url = BASE_URL + '/auth/login'
        csrf_url = BASE_URL + '/api/auth/csrf'
        auth_url = BASE_URL + '/api/auth'

        # fetch login page
        self.session.get(login_url)

        # get csrf token
        self.session.post(csrf_url)
        self.csrf = self.session.cookies.get('csrf')

        # try to login
        form_data = {
            'action': 'login',
            'username': self.username,
            'password': self.password,
            'csrf': self.csrf,
            'next': BASE_URL
        }

        self.session.post(auth_url, data=form_data)
        self.auth_refresh = self.session.cookies.get('auth-refresh')

    def fetch_auth_token(self):
        """
        Fetch authorization token for the user
        :return:
        """
        session_url = BASE_URL + '/api/auth/session'

        response = self.session.post(session_url)
        if response.status_code == 200:
            self.token = response.json().get('token')
        else:
            print(response.text)

    def initiate_new_query(self, query_id, query):
        """
        Initiates a new query
        """

        query_data = {
            "operationName": "UpsertQuery",
            "variables": {
                "favs_last_24h": False,
                "favs_last_7d": False,
                "favs_last_30d": False,
                "favs_all_time": True,
                "object": {
                    "id": query_id,
                    "schedule": None,
                    "dataset_id": 4,
                    "name": "GPv2 - user based metrics",
                    "query": query,
                    "user_id": 84,
                    "description": "",
                    "is_archived": False,
                    "is_temp": False,
                    "tags": [],
                    "parameters": [],
                    "visualizations": {
                        "data": [],
                        "on_conflict": {
                            "constraint": "visualizations_pkey",
                            "update_columns": ["name", "options"]
                        }
                    }
                },
                "on_conflict": {
                    "constraint": "queries_pkey",
                    "update_columns": ["dataset_id", "name", "description", "query", "schedule",
                                       "is_archived", "is_temp", "tags", "parameters"]
                },
                "session_id": 84
            },
            # pylint: disable=line-too-long
            "query": "mutation UpsertQuery($session_id: Int!, $object: queries_insert_input!, $on_conflict: queries_on_conflict!, $favs_last_24h: Boolean! = false, $favs_last_7d: Boolean! = false, $favs_last_30d: Boolean! = false, $favs_all_time: Boolean! = true) {\n  insert_queries_one(object: $object, on_conflict: $on_conflict) {\n    ...Query\n    favorite_queries(where: {user_id: {_eq: $session_id}}, limit: 1) {\n      created_at\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment Query on queries {\n  id\n  dataset_id\n  name\n  description\n  query\n  private_to_group_id\n  is_temp\n  is_archived\n  created_at\n  updated_at\n  schedule\n  tags\n  parameters\n  user {\n    ...User\n    __typename\n  }\n  visualizations {\n    id\n    type\n    name\n    options\n    created_at\n    __typename\n  }\n  query_favorite_count_all @include(if: $favs_all_time) {\n    favorite_count\n    __typename\n  }\n  query_favorite_count_last_24h @include(if: $favs_last_24h) {\n    favorite_count\n    __typename\n  }\n  query_favorite_count_last_7d @include(if: $favs_last_7d) {\n    favorite_count\n    __typename\n  }\n  query_favorite_count_last_30d @include(if: $favs_last_30d) {\n    favorite_count\n    __typename\n  }\n  __typename\n}\n\nfragment User on users {\n  id\n  name\n  profile_image_url\n  __typename\n}\n"
        }

        self.session.headers.update({'authorization': f'Bearer {self.token}'})

        response = self.session.post(GRAPH_URL, json=query_data)
        if response.status_code == 200:
            data = response.json()
            print("New query has been posted with response:")
            print(data)
        else:
            print(response.text)

    def execute_query(self, query_id):
        """
        Executes query according to the given id.
        """
        query_data = {
            "operationName": "ExecuteQuery",
            "variables": {
                "query_id": query_id,
                "parameters": []
            },
            "query":
                "mutation ExecuteQuery($query_id: Int!, $parameters: [Parameter!]!)"
                "{\n  execute_query(query_id: $query_id, parameters: $parameters) "
                "{\n    job_id\n    __typename\n  }\n}\n"}

        self.session.headers.update({'authorization': f'Bearer {self.token}'})
        response = self.session.post(GRAPH_URL, json=query_data)
        if response.status_code == 200:
            data = response.json()
            print("query executed successfully with response:")
            print(data)
        else:
            print(response.text)

    def query_result_id(self, query_id):
        """
        Fetch the query result id for a query
        :param query_id: provide the query_id
        :return:
        """
        query_data = {
            "operationName": "GetResult",
            "variables": {"query_id": query_id},
            "query": "query GetResult($query_id: Int!, $parameters: [Parameter!]) "
                     "{\n  get_result(query_id: $query_id, parameters: $parameters) "
                     "{\n    job_id\n    result_id\n    __typename\n  }\n}\n"
        }

        self.session.headers.update({'authorization': f'Bearer {self.token}'})

        response = self.session.post(GRAPH_URL, json=query_data)
        if response.status_code == 200:
            data = response.json()
            if 'errors' in data:
                return None
            result_id = data.get('data').get('get_result').get('result_id')
            return result_id
        print("Unsuccessful response", response.text)
        return None

    def query_result(self, result_id):
        """
        Fetch the result for a query
        :param result_id: result id of the query
        :return:
        """
        query_data = {
            "operationName": "FindResultDataByResult",
            "variables": {"result_id": result_id},
            # TODO - there should be a prettier format for this without the whitespace.
            "query": "query FindResultDataByResult($result_id: uuid!) "
                     "{\n  query_results(where: {id: {_eq: $result_id}}) "
                     "{\n    id\n    job_id\n    error\n    runtime\n    "
                     "generated_at\n    columns\n    __typename\n  }"
                     "\n  get_result_by_result_id(args: {want_result_id: $result_id}) "
                     "{\n    data\n    __typename\n  }\n}\n"
        }

        self.session.headers.update({'authorization': f'Bearer {self.token}'})

        response = self.session.post(GRAPH_URL, json=query_data)
        if response.status_code == 200:
            data = response.json()
            return data
        print("Unsuccessful response", response.text)
        return {}
