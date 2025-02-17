import pandas as pd
from psycopg2 import errors
from sqlFunc import SqlFunction


class userUpdateHandler(SqlFunction):
    def __init__(self, db_name, db_user, db_pass, db_host, db_port, force: dict, user_type: str = "Branch", is_genesys: bool = False, password: str = "Adira123@", division: str = ''):
        super().__init__(db_name, db_user, db_pass, db_host, db_port)
        self.FORCE_UPDATE = force
        self.USER_TYPE = user_type
        self.IS_GENESYS = is_genesys
        self.PASSWORD = password
        self.DIVISION_ID = division
        self.data_changed_logs = []

    def update_all(self, df: pd.DataFrame):
        """Ini function buat update all le."""
        data_user = []
        data_branch = []
        data_group = []
        data_role = []
        columns = list(df.columns)
        generated_number = 0

        for row in df.itertuples(index=False, name=None):
            generated_number += 1
            print("============================================")
            data_user.append(self.update_user(row, columns, generated_number))
            print("[USERS] Finish Updating Users...\n")
            data_group.append(self.update_group(row, columns, generated_number))
            print("[GROUPS] Finish Updating Group...\n")
            data_branch.append(self.update_master_branch(row, columns, generated_number))
            print("[MASTER-BRANCH] Finish Updating Branch...\n")
            data_role.append(self.update_role(row, columns, generated_number))
            print("[ROLES] Finish Updating Roles...\n")
            print("============================================\n")
        
        self.update_group_has_branch(data_branch, data_group)
        self.update_user_has_branch(data_user, data_branch)
        self.update_user_has_roles(data_user, data_role)
        self.update_user_has_group(data_user, data_group)


    def update_user(self, row: tuple, col: list, generated_number: int):
        """Ini function buat update user le."""
        name = row[col.index("Name")]
        email = row[col.index("Email")]
        nip = row[col.index("NIP")]
        username = email
        phone = "00000000"
        formatted_number = f"US{generated_number:04d}"
        hashed_password = self.hash_password(self.PASSWORD)
        existing_id = self.read_data("users", "name", name)
        try:
            if existing_id is not None and self.FORCE_UPDATE['users'] is False:
                print(f"[USERS] Skipping existing record with Name: {name}")
                return existing_id['user_id']
            elif existing_id is not None:
                data = self.update_data(
                        "users",
                        [
                            "name",
                            "email",
                            "password",
                            "is_active",
                            "number",
                            "tipe",
                            "nik",
                            "username",
                            "phone",
                            "is_genesys",
                            "created_by",
                            "updated_by"
                        ],
                        [
                            name,
                            email,
                            hashed_password,
                            1,
                            formatted_number,
                            self.USER_TYPE,
                            nip,
                            username,
                            phone,
                            self.IS_GENESYS,
                            "kuro@kuro.com",
                            "kuro@kuro.com"
                        ],
                        "email",
                        email
                    )
                print(f"[USERS] Updated existing record with Name: {data['name']}")
                return data['user_id']
            else:
                data = self.create_data(
                    "users",
                    [
                        "name",
                        "email",
                        "password",
                        "is_active",
                        "number",
                        "tipe",
                        "nik",
                        "username",
                        "phone",
                        "is_genesys",
                        "created_by",
                        "updated_by"
                    ],
                    [
                        name,
                        email,
                        hashed_password,
                        1,
                        formatted_number,
                        self.USER_TYPE,
                        nip,
                        username,
                        phone,
                        self.IS_GENESYS,
                        "kuro@kuro.com",
                        "kuro@kuro.com"
                    ]
                )
                print(f"[USERS] Inserted new row with Name: {data['name']}")
                return data['user_id']
        except Exception as e:
            print("[USERS] Error: ", e)
            raise InterruptedError(e)
                    
    def update_master_branch(self, row: tuple, col: list, generated_number: int):
        """Ini function buat update master_branch le."""
        name = row[col.index("DEPT / BRANCH")]
        formatted_number = f"{generated_number+1000:04d}"
        existing_id = self.read_data("master_branch", "branch_name", name)
        try:
            if existing_id is not None and self.FORCE_UPDATE['master_branch'] is False:
                print(f"[MASTER-BRANCH] Skipping existing record with Name: {name}")
                return existing_id['id']
            elif existing_id is not None:
                data = self.update_data(
                        "master_branch",
                        [
                            "branch_code",
                            "branch_name"
                        ],
                        [
                            formatted_number,
                            name
                        ],
                        "branch_name",
                        name
                    )
                print(f"[MASTER-BRANCH] Updated existing record with Name: {data['branch_name']}")
                return data['id']
            else:
                data = self.create_data(
                    "master_branch",
                    [
                        "branch_code",
                        "branch_name"
                    ],
                    [
                        formatted_number,
                        name
                    ]
                )
                print(f"[MASTER-BRANCH] Inserted new row with Name: {data['branch_name']}")
                return data['id']
        except Exception as e:
            print("[MASTER-BRANCH] Error: ", e)
            raise InterruptedError(e)
                    
    def update_group(self, row: tuple, col: list, generated_number: int):
        """Ini function buat update group le."""
        name = row[col.index("Personnel Area")].split("Wilayah Area ")[1].upper()
        formatted_number = f"GR{generated_number:04d}"
        existing_id = self.read_data("groups", "name", name)
        try:
            if existing_id is not None and self.FORCE_UPDATE['groups'] is False:
                print(f"[GROUPS] Skipping existing record with Name: {name}")
                return existing_id['id']
            elif existing_id is not None:
                data = self.update_data(
                    "groups",
                    [
                        "division_id",
                        "number",
                        "name",
                        "status",
                        "created_by",
                        "updated_by"
                    ],
                    [
                        self.DIVISION_ID,
                        existing_id['number'],
                        name,
                        'active',
                        "kuro@kuro.com",
                        "kuro@kuro.com"
                    ],
                    "name",
                    name
                )
                print(f"[GROUPS] Updated existing record with Name: {data['name']}")
                return data['id']
            else:
                data = self.create_data(
                    "groups",
                    [
                        "division_id",
                        "number",
                        "name",
                        "status",
                        "created_by",
                        "updated_by"
                    ],
                    [
                        self.DIVISION_ID,
                        formatted_number,
                        name,
                        'active',
                        "kuro@kuro.com",
                        "kuro@kuro.com"
                    ]
                )
                print(f"[GROUPS] Inserted new row with Name: {data['name']}")
                return data['id']
        except Exception as e:
            print("[GROUPS] Error: ", e)
            raise InterruptedError(e)
    
    def update_role(self, row: tuple, col: list, generated_number: int):
        """Ini function buat update role le."""
        name = row[col.index("JOB")].title()
        formatted_number = f"ROLE{generated_number+1000:04d}"
        existing_id = self.read_data("roles", "name", name)
        try:
            if existing_id is not None and self.FORCE_UPDATE['roles'] is False:
                print(f"[ROLES] Skipping existing record with Name: {name}")
                return existing_id['id']
            elif existing_id is not None:
                data = self.update_data(
                    "roles",
                    [
                        "team_id",
                        "name",
                        "guard_name",
                        "number"
                    ],
                    [
                        '00000000-0000-0000-0000-000000000000',
                        name,
                        'api',
                        formatted_number
                    ],
                    "name",
                    name
                )
                print(f"[ROLES] Updated existing record with Name: {data['name']}")
                return data['id']
            else:
                data = self.create_data(
                    "roles",
                    [
                        "team_id",
                        "name",
                        "guard_name",
                        "number"
                    ],
                    [
                        '00000000-0000-0000-0000-000000000000',
                        name,
                        'api',
                        formatted_number
                    ]
                )
                print(f"[ROLES] Inserted new row with Name: {data['name']}")
                return data['id']
        except Exception as e:
            print("[ROLES] Error: ", e)
            raise InterruptedError(e)

    def update_group_has_branch(self, branch_id: list, group_id: list):
        """Ini function buat update group_hasMany_branch le."""
        data_id_updated = []
        if any(not self.read_data('master_branch', 'id', data) for data in branch_id):
            return False
        if any(not self.read_data('groups', 'id', data) for data in group_id):
            return False
        try:
            for branch, group in zip(branch_id, group_id):
                result = self.update_data(
                    'master_branch',
                    ['group_id'],
                    [group],
                    'id',
                    branch
                )
                print(f"[GROUP-HAS-BRANCH] Updating Branch Group by Branch ID: {branch}")
                data_id_updated.append(zip(branch_id, group_id))
            return True
        except Exception as e:
            print("[GROUP-HAS-BRANCH] Error: ", e)
            raise InterruptedError(e)
        
    def update_user_has_branch(self, user_id_list: list, branch_id_list: list):
        """Ini function buat update user_hasMany_branch le."""

        if any(not self.read_data('users', 'user_id', data) for data in user_id_list):
            return False
        if any(not self.read_data('master_branch', 'id', data) for data in branch_id_list):
            return False

        existing_pairs = {(row["user_id"], row["branch_id"]) for row in self.reads_data('user_has_branch')}
        for user_id, branch_id in zip(user_id_list, branch_id_list):
            user_exists = any(row[0] == user_id for row in existing_pairs)

            if user_exists:
                if (user_id, branch_id) in existing_pairs:
                    print(f"[USER-HAS-BRANCH] Skipping existing user-branch pair: ({user_id}, {branch_id})")
                    continue

            result = self.create_data('user_has_branch', 
                ['user_id', 'branch_id'], 
                [user_id, branch_id]
            )
            if result:
                print(f"[USER-HAS-BRANCH] Inserted new user-branch pair: ({user_id}, {branch_id})")

        return True
    
    def update_user_has_roles(self, user_id_list: list, roles_id_list: list):
        """Ini function buat update user_hasOne_roles le."""

        if any(not self.read_data('users', 'user_id', data) for data in user_id_list):
            return False
        if any(not self.read_data('roles', 'id', data) for data in roles_id_list):
            return False

        existing_pairs = {(row["role_id"], row["model_id"]) for row in self.reads_data('model_has_roles')}
        for role_id, model_id in zip(roles_id_list, user_id_list):
            # Check if user_id exists in model_has_roles
            user_exists = any(row[1] == model_id for row in existing_pairs)

            if user_exists:
                # Check if user_id already has the same roles_id
                if (role_id, model_id) in existing_pairs:
                    print(f"[USER-HAS-ROLES] Skipping existing user-roles pair: ({role_id}, {model_id})")
                    continue  # Skip inserting

            # Insert new row if the pair does not exist
            result = self.create_data(
                'model_has_roles', 
                ['role_id', 'model_type', 'model_id', 'team_id'], 
                [role_id, r'App\Models\User', model_id, '00000000-0000-0000-0000-000000000000']
            )
            
            if result:
                print(f"[USER-HAS-ROLES] Inserted new user-roles pair: ({role_id}, {model_id})")

        return True
    
    def update_user_has_group(self, user_id_list: list, group_id_list: list):
        """Ini function buat update user_hasOne_group le."""

        if any(not self.read_data('users', 'user_id', data) for data in user_id_list):
            return False
        if any(not self.read_data('groups', 'id', data) for data in group_id_list):
            return False

        existing_pairs = {(row["user_id"], row["group_id"]) for row in self.reads_data('user_has_group')}
        for user_id, group_id in zip(user_id_list, group_id_list):
            user_exists = any(row[0] == user_id for row in existing_pairs)

            if user_exists:
                if (user_id, group_id) in existing_pairs:
                    print(f"[USER-HAS-GROUPS] Skipping existing user-group pair: ({user_id}, {group_id})")
                    continue

            result = self.create_data('user_has_group', 
                ['user_id', 'branch_id'], 
                [user_id, group_id]
            )
            if result:
                print(f"[USER-HAS-GROUPS] Inserted new user-group pair: ({user_id}, {group_id})")

        return True