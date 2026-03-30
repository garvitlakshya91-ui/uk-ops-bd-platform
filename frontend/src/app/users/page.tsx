'use client';

import React, { useState, useEffect } from 'react';
import {
  UserGroupIcon,
  ShieldCheckIcon,
  CheckCircleIcon,
  XCircleIcon,
} from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import RoleGate from '@/components/rbac/RoleGate';
import { useAuth } from '@/lib/auth';
import api from '@/lib/api';
import type { UserRecord } from '@/lib/api';
import toast from 'react-hot-toast';

const ROLE_OPTIONS = [
  { value: 'admin', label: 'Admin' },
  { value: 'bd_manager', label: 'BD Manager' },
  { value: 'bd_analyst', label: 'BD Analyst' },
  { value: 'viewer', label: 'Viewer' },
];

const ROLE_BADGE_COLORS: Record<string, string> = {
  admin: 'bg-red-500/20 text-red-400 border-red-500/30',
  bd_manager: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  bd_analyst: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
  viewer: 'bg-slate-500/20 text-slate-400 border-slate-500/30',
};

function formatDate(dateStr: string): string {
  if (!dateStr) return '--';
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return '--';
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}

function AccessDenied() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh]">
      <div className="p-4 bg-red-500/10 rounded-full mb-4">
        <ShieldCheckIcon className="w-12 h-12 text-red-400" />
      </div>
      <h1 className="text-2xl font-bold text-white mb-2">Access Denied</h1>
      <p className="text-slate-400 text-sm">You do not have permission to view this page.</p>
    </div>
  );
}

function UsersContent() {
  const { user: currentUser } = useAuth();
  const [users, setUsers] = useState<UserRecord[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/users')
      .then((res) => {
        const data = res.data;
        const items = (Array.isArray(data) ? data : data?.items || []).map((u: any) => ({
          id: u.id || '',
          email: u.email || '',
          name: u.name || '',
          role: u.role || 'viewer',
          status: u.status || 'active',
          created_at: u.created_at || '',
        }));
        setUsers(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  const handleRoleChange = (userId: string, newRole: string) => {
    if (userId === currentUser?.id) {
      toast.error('You cannot change your own role');
      return;
    }
    setUsers((prev) => prev.map((u) => (u.id === userId ? { ...u, role: newRole } : u)));
    api.put(`/users/${userId}`, { role: newRole })
      .then(() => toast.success('Role updated successfully'))
      .catch(() => {
        toast.error('Failed to update role');
        // Revert
        api.get('/users').then((res) => {
          const data = res.data;
          setUsers(Array.isArray(data) ? data : data?.items || []);
        });
      });
  };

  const handleToggleStatus = (userId: string, currentStatus: string) => {
    if (userId === currentUser?.id) {
      toast.error('You cannot deactivate yourself');
      return;
    }
    const newStatus = currentStatus === 'active' ? 'inactive' : 'active';
    setUsers((prev) => prev.map((u) => (u.id === userId ? { ...u, status: newStatus as any } : u)));

    const endpoint = newStatus === 'active' ? `/users/${userId}/activate` : `/users/${userId}/deactivate`;
    api.put(endpoint)
      .then(() => toast.success(`User ${newStatus === 'active' ? 'activated' : 'deactivated'}`))
      .catch(() => {
        toast.error('Failed to update user status');
        api.get('/users').then((res) => {
          const data = res.data;
          setUsers(Array.isArray(data) ? data : data?.items || []);
        });
      });
  };

  const activeCount = users.filter((u) => u.status === 'active').length;
  const adminCount = users.filter((u) => u.role === 'admin').length;

  if (loading && users.length === 0) {
    return (
      <div className="space-y-6">
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-red-600 via-rose-600 to-pink-600 p-8">
          <div className="relative">
            <h1 className="text-3xl font-bold text-white tracking-tight">User Management</h1>
            <p className="text-rose-100 mt-1 text-sm">Loading users...</p>
          </div>
        </div>
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <div key={i} className="bg-slate-800 border border-slate-700/50 rounded-xl p-4 animate-pulse">
              <div className="h-4 bg-slate-700 rounded w-2/3 mb-2" />
              <div className="h-3 bg-slate-700 rounded w-full" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Gradient Header */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-red-600 via-rose-600 to-pink-600 p-8">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wNSI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-40" />
        <div className="relative">
          <h1 className="text-3xl font-bold text-white tracking-tight">User Management</h1>
          <p className="text-rose-100 mt-1 text-sm">Manage user accounts, roles, and access permissions</p>
        </div>
        <div className="relative flex flex-wrap items-center gap-2 mt-5">
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/15 backdrop-blur-sm text-white text-sm font-semibold border border-white/20">
            {users.length} total users
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/10 backdrop-blur-sm text-rose-100 text-sm font-medium border border-white/10">
            {activeCount} active
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/10 backdrop-blur-sm text-rose-100 text-sm font-medium border border-white/10">
            {adminCount} admins
          </span>
        </div>
      </div>

      {/* Users Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">User</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Email</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Role</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Created</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {users.map((u) => (
                <tr key={u.id} className="hover:bg-slate-700/50 transition-colors">
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-purple-600 rounded-full flex items-center justify-center text-xs font-bold text-white">
                        {(u.name || '?')
                          .split(' ')
                          .map((n) => n[0])
                          .join('')
                          .toUpperCase()
                          .slice(0, 2)}
                      </div>
                      <span className="font-medium text-white">{u.name}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-slate-400 text-xs">{u.email}</td>
                  <td className="px-4 py-3">
                    <select
                      value={u.role}
                      onChange={(e) => handleRoleChange(u.id, e.target.value)}
                      disabled={u.id === currentUser?.id}
                      className={cn(
                        'px-2 py-1 text-xs font-medium rounded-lg border bg-slate-800 focus:outline-none focus:ring-2 focus:ring-blue-500/40',
                        ROLE_BADGE_COLORS[u.role] || ROLE_BADGE_COLORS.viewer,
                        u.id === currentUser?.id && 'opacity-60 cursor-not-allowed'
                      )}
                    >
                      {ROLE_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td className="px-4 py-3">
                    {u.status === 'active' ? (
                      <Badge variant="bg-emerald-500/20 text-emerald-400 border-emerald-500/30" icon={<CheckCircleIcon className="w-3.5 h-3.5" />}>
                        Active
                      </Badge>
                    ) : (
                      <Badge variant="bg-red-500/20 text-red-400 border-red-500/30" icon={<XCircleIcon className="w-3.5 h-3.5" />}>
                        Inactive
                      </Badge>
                    )}
                  </td>
                  <td className="px-4 py-3 text-slate-400 text-xs">{formatDate(u.created_at)}</td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => handleToggleStatus(u.id, u.status)}
                      disabled={u.id === currentUser?.id}
                      className={cn(
                        'px-3 py-1.5 text-xs font-medium rounded-lg transition-colors',
                        u.status === 'active'
                          ? 'text-red-400 bg-red-500/10 hover:bg-red-500/20'
                          : 'text-emerald-400 bg-emerald-500/10 hover:bg-emerald-500/20',
                        u.id === currentUser?.id && 'opacity-40 cursor-not-allowed'
                      )}
                    >
                      {u.status === 'active' ? 'Deactivate' : 'Activate'}
                    </button>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-12 text-center text-slate-500">
                    <UserGroupIcon className="w-12 h-12 mx-auto mb-3 opacity-50" />
                    <p>No users found</p>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

export default function UsersPage() {
  return (
    <RoleGate roles={['admin']} fallback={<AccessDenied />}>
      <UsersContent />
    </RoleGate>
  );
}
