'use client';

import React from 'react';
import { useAuth } from '@/lib/auth';
import type { Role } from '@/lib/permissions';

interface RoleGateProps {
  roles: Role[];
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

export default function RoleGate({ roles, children, fallback = null }: RoleGateProps) {
  const { user } = useAuth();

  if (!user || !roles.includes(user.role as Role)) {
    return <>{fallback}</>;
  }

  return <>{children}</>;
}
