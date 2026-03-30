import type { ComponentType, SVGProps } from 'react';

export type Role = 'admin' | 'bd_manager' | 'bd_analyst' | 'viewer';
export type Resource = 'dashboard' | 'applications' | 'pipeline' | 'schemes' | 'contracts' | 'companies' | 'alerts' | 'scrapers' | 'users';
export type Action = 'view' | 'create' | 'edit' | 'delete' | 'export';

export interface NavItem {
  href: string;
  label: string;
  icon: ComponentType<SVGProps<SVGSVGElement>>;
}

const PERMISSIONS: Record<Role, Record<Resource, Action[]>> = {
  admin: {
    dashboard: ['view', 'export'],
    applications: ['view', 'create', 'edit', 'delete', 'export'],
    pipeline: ['view', 'create', 'edit', 'delete', 'export'],
    schemes: ['view', 'create', 'edit', 'delete', 'export'],
    contracts: ['view', 'create', 'edit', 'delete', 'export'],
    companies: ['view', 'create', 'edit', 'delete', 'export'],
    alerts: ['view', 'create', 'edit', 'delete'],
    scrapers: ['view', 'create', 'edit', 'delete'],
    users: ['view', 'create', 'edit', 'delete'],
  },
  bd_manager: {
    dashboard: ['view', 'export'],
    applications: ['view', 'create', 'edit', 'delete', 'export'],
    pipeline: ['view', 'create', 'edit', 'delete', 'export'],
    schemes: ['view', 'create', 'edit', 'delete', 'export'],
    contracts: ['view', 'create', 'edit', 'delete', 'export'],
    companies: ['view', 'create', 'edit', 'delete', 'export'],
    alerts: ['view', 'create', 'edit', 'delete'],
    scrapers: ['view'],
    users: [],
  },
  bd_analyst: {
    dashboard: ['view', 'export'],
    applications: ['view', 'create', 'edit', 'export'],
    pipeline: ['view', 'create', 'edit'],
    schemes: ['view', 'edit', 'export'],
    contracts: ['view', 'export'],
    companies: ['view', 'create', 'edit'],
    alerts: ['view', 'create'],
    scrapers: [],
    users: [],
  },
  viewer: {
    dashboard: ['view'],
    applications: ['view'],
    pipeline: [],
    schemes: ['view'],
    contracts: ['view'],
    companies: ['view'],
    alerts: ['view'],
    scrapers: [],
    users: [],
  },
};

export function hasPermission(role: Role | undefined, resource: Resource, action: Action): boolean {
  if (!role) return false;
  const rolePermissions = PERMISSIONS[role];
  if (!rolePermissions) return false;
  const resourceActions = rolePermissions[resource];
  if (!resourceActions) return false;
  return resourceActions.includes(action);
}

const PATH_TO_RESOURCE: Record<string, Resource> = {
  '/dashboard': 'dashboard',
  '/applications': 'applications',
  '/pipeline': 'pipeline',
  '/schemes': 'schemes',
  '/contracts': 'contracts',
  '/companies': 'companies',
  '/alerts': 'alerts',
  '/scraper-health': 'scrapers',
  '/users': 'users',
};

export function canAccessPage(role: Role | undefined, path: string): boolean {
  if (!role) return false;
  const resource = PATH_TO_RESOURCE[path];
  if (!resource) return true; // unknown paths are allowed by default
  return hasPermission(role, resource, 'view');
}

export function getVisibleNavItems(role: Role | undefined, allNavItems: NavItem[]): NavItem[] {
  if (!role) return [];
  return allNavItems.filter((item) => {
    const resource = PATH_TO_RESOURCE[item.href];
    if (!resource) return true;
    return hasPermission(role, resource, 'view');
  });
}
