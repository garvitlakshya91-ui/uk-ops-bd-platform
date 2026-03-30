'use client';

import React from 'react';
import { useAuth } from '@/lib/auth';
import { hasPermission } from '@/lib/permissions';
import type { Resource, Action } from '@/lib/permissions';

interface PermissionGateProps {
  resource: Resource;
  action: Action;
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

export default function PermissionGate({ resource, action, children, fallback = null }: PermissionGateProps) {
  const { user } = useAuth();

  if (!user || !hasPermission(user.role as any, resource, action)) {
    return <>{fallback}</>;
  }

  return <>{children}</>;
}
