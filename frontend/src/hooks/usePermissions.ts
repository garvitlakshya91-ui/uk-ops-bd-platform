'use client';

import { useAuth } from '@/lib/auth';
import { hasPermission } from '@/lib/permissions';
import type { Role, Resource, Action } from '@/lib/permissions';

export function usePermissions() {
  const { user } = useAuth();
  const role = user?.role as Role | undefined;

  return {
    can: (resource: Resource, action: Action) => hasPermission(role, resource, action),
    isAdmin: role === 'admin',
    isManager: role === 'bd_manager',
    isAnalyst: role === 'bd_analyst',
    isViewer: role === 'viewer',
    role,
  };
}
