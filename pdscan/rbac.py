"""
Role-Based Access Control (RBAC) for PDScan
"""

from typing import Dict, List, Set, Optional

# Định nghĩa các quyền (permissions)
PERMISSIONS = {
    'scan': 'Quét dữ liệu',
    'view_logs': 'Xem log',
    'manage_users': 'Quản lý người dùng',
    'manage_config': 'Quản lý cấu hình',
    'view_reports': 'Xem báo cáo',
    'export_data': 'Xuất dữ liệu',
}

# Định nghĩa các vai trò (roles) và quyền tương ứng
ROLES = {
    'admin': {'scan', 'view_logs', 'manage_users', 'manage_config', 'view_reports', 'export_data'},
    'user': {'scan', 'view_reports', 'export_data'},
    'viewer': {'view_reports'},
}

class RBACManager:
    """Quản lý RBAC cho user"""
    def __init__(self):
        # user_id -> role
        self.user_roles: Dict[str, str] = {}

    def assign_role(self, user_id: str, role: str) -> bool:
        if role not in ROLES:
            return False
        self.user_roles[user_id] = role
        return True

    def get_role(self, user_id: str) -> Optional[str]:
        return self.user_roles.get(user_id)

    def get_permissions(self, user_id: str) -> Set[str]:
        role = self.get_role(user_id)
        if role and role in ROLES:
            return ROLES[role]
        return set()

    def check_permission(self, user_id: str, permission: str) -> bool:
        return permission in self.get_permissions(user_id)

    def list_users(self) -> List[str]:
        return list(self.user_roles.keys())

    def list_roles(self) -> List[str]:
        return list(ROLES.keys())

    def list_permissions(self) -> List[str]:
        return list(PERMISSIONS.keys()) 