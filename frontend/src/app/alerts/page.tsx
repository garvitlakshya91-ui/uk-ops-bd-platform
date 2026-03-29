'use client';

import React, { useState, useEffect } from 'react';
import {
  DocumentTextIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  InformationCircleIcon,
  BuildingOffice2Icon,
  CheckCircleIcon,
  BellAlertIcon,
  Cog6ToothIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { cn, formatRelativeDate } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import Select from '@/components/ui/Select';
import api from '@/lib/api';

interface AlertItem {
  id: string;
  type: string;
  title: string;
  message: string;
  timestamp: string;
  read: boolean;
  severity: 'critical' | 'warning' | 'info';
}

/* Alerts loaded from API */

const typeConfig: Record<string, { icon: React.ElementType; color: string; label: string; borderColor: string }> = {
  new_application: { icon: DocumentTextIcon, color: 'text-blue-400 bg-blue-500/10', label: 'New Application', borderColor: 'border-l-blue-500' },
  contract_expiring: { icon: ClockIcon, color: 'text-amber-400 bg-amber-500/10', label: 'Contract Expiring', borderColor: 'border-l-amber-500' },
  scraper_failure: { icon: ExclamationTriangleIcon, color: 'text-red-400 bg-red-500/10', label: 'Scraper Issue', borderColor: 'border-l-red-500' },
  stage_change: { icon: InformationCircleIcon, color: 'text-violet-400 bg-violet-500/10', label: 'Stage Change', borderColor: 'border-l-violet-500' },
  duplicate_company: { icon: BuildingOffice2Icon, color: 'text-cyan-400 bg-cyan-500/10', label: 'Duplicate', borderColor: 'border-l-cyan-500' },
  high_score: { icon: BellAlertIcon, color: 'text-orange-400 bg-orange-500/10', label: 'High Score', borderColor: 'border-l-orange-500' },
};

const severityConfig: Record<string, { label: string; variant: string }> = {
  critical: { label: 'Critical', variant: 'bg-red-500/20 text-red-400 border-red-500/30' },
  warning: { label: 'Warning', variant: 'bg-amber-500/20 text-amber-400 border-amber-500/30' },
  info: { label: 'Info', variant: 'bg-blue-500/20 text-blue-300 border-blue-500/30' },
};

function getDateGroup(timestamp: string): string {
  if (!timestamp) return 'Older';
  const date = new Date(timestamp);
  if (isNaN(date.getTime())) return 'Older';
  const today = new Date();
  const diffDays = Math.floor((today.getTime() - date.getTime()) / (1000 * 60 * 60 * 24));
  if (diffDays === 0) return 'Today';
  if (diffDays === 1) return 'Yesterday';
  if (diffDays <= 6) return 'Earlier this week';
  return 'Older';
}

export default function AlertsPage() {
  const [typeFilter, setTypeFilter] = useState('');
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    const params: Record<string, string> = {};
    if (typeFilter) params.type = typeFilter;

    api.get('/v2/alerts', { params })
      .then(res => {
        const data = res.data;
        const items = (Array.isArray(data) ? data : data?.items || []).map((a: any) => ({
          ...a,
          id: a.id || '',
          type: a.type || 'new_application',
          title: a.title || '',
          message: a.message || '',
          timestamp: a.timestamp || '',
          read: a.read ?? false,
          severity: a.severity || (a.type === 'scraper_failure' || a.type === 'high_score' ? 'critical' : a.type === 'contract_expiring' ? 'warning' : 'info'),
        }));
        setAlerts(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [typeFilter]);

  const filtered = alerts.filter((a) => !typeFilter || a.type === typeFilter);
  const unreadCount = alerts.filter((a) => !a.read).length;
  const todayCount = alerts.filter((a) => getDateGroup(a.timestamp) === 'Today').length;
  const weekCount = alerts.filter((a) => getDateGroup(a.timestamp) !== 'Older').length;

  const markRead = (id: string) => {
    setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, read: true } : a)));
    api.put(`/alerts/${id}/read`).catch(() => {});
  };

  const dismissAlert = (id: string) => {
    setAlerts((prev) => prev.filter((a) => a.id !== id));
  };

  const markSelectedRead = () => {
    setAlerts((prev) => prev.map((a) => (selectedIds.has(a.id) ? { ...a, read: true } : a)));
    setSelectedIds(new Set());
  };

  const markAllRead = () => {
    setAlerts((prev) => prev.map((a) => ({ ...a, read: true })));
    setSelectedIds(new Set());
  };

  const toggleSelect = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Group alerts by date
  const groupedAlerts: { label: string; alerts: typeof filtered }[] = [];
  const groupOrder = ['Today', 'Yesterday', 'Earlier this week', 'Older'];
  for (const group of groupOrder) {
    const groupAlerts = filtered.filter((a) => getDateGroup(a.timestamp) === group);
    if (groupAlerts.length > 0) {
      groupedAlerts.push({ label: group, alerts: groupAlerts });
    }
  }

  if (loading && alerts.length === 0) {
    return (
      <div className="space-y-6">
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-amber-600 via-orange-600 to-red-600 p-8">
          <div className="relative">
            <h1 className="text-3xl font-bold text-white tracking-tight">Intelligence Alerts</h1>
            <p className="text-orange-100 mt-1 text-sm">Loading alerts...</p>
          </div>
        </div>
        <div className="space-y-3">
          {[1, 2, 3, 4].map((i) => (
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
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-amber-600 via-orange-600 to-red-600 p-8">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wNSI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-40" />
        <div className="relative flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold text-white tracking-tight">Intelligence Alerts</h1>
            <p className="text-orange-100 mt-1 text-sm">Stay ahead of planning developments and opportunities</p>
            <div className="flex items-center gap-2 mt-4">
              <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/15 backdrop-blur-sm text-white text-sm font-semibold border border-white/20">
                {unreadCount} unread
              </span>
              <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/10 backdrop-blur-sm text-orange-100 text-sm font-medium border border-white/10">
                {todayCount} today
              </span>
              <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/10 backdrop-blur-sm text-orange-100 text-sm font-medium border border-white/10">
                {weekCount} this week
              </span>
            </div>
          </div>
          <button className="p-2.5 rounded-lg bg-white/10 hover:bg-white/20 text-white transition-colors border border-white/10">
            <Cog6ToothIcon className="w-5 h-5" />
          </button>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <Select
          options={[
            { value: 'new_application', label: 'New Application' },
            { value: 'contract_expiring', label: 'Contract Expiring' },
            { value: 'scraper_failure', label: 'Scraper Issue' },
            { value: 'stage_change', label: 'Stage Change' },
            { value: 'duplicate_company', label: 'Duplicate' },
            { value: 'high_score', label: 'High Score' },
          ]}
          value={typeFilter}
          onChange={setTypeFilter}
          placeholder="All Types"
          className="w-48"
        />
        {typeFilter && (
          <button onClick={() => setTypeFilter('')} className="text-xs text-slate-400 hover:text-white">
            Clear
          </button>
        )}
        <div className="ml-auto flex items-center gap-2">
          {selectedIds.size > 0 && (
            <button
              onClick={markSelectedRead}
              className="px-3 py-1.5 text-sm font-medium text-blue-400 bg-blue-500/10 rounded-lg hover:bg-blue-500/20 transition-colors"
            >
              Mark {selectedIds.size} read
            </button>
          )}
          {unreadCount > 0 && (
            <button
              onClick={markAllRead}
              className="px-3 py-1.5 text-sm font-medium text-slate-400 bg-slate-700 rounded-lg hover:bg-slate-600 transition-colors"
            >
              Mark all read
            </button>
          )}
        </div>
      </div>

      {/* Grouped Alerts */}
      <div className="space-y-6">
        {groupedAlerts.map((group) => (
          <div key={group.label}>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-3 px-1">{group.label}</h3>
            <div className="space-y-3">
              {group.alerts.map((alert) => {
                const config = typeConfig[alert.type || ''] || typeConfig.new_application;
                const severity = severityConfig[alert.severity || ''] || severityConfig.info;
                const Icon = config.icon;
                return (
                  <div
                    key={alert.id}
                    className={cn(
                      'bg-slate-800 border rounded-xl p-4 transition-all hover:border-slate-600 border-l-4',
                      alert.read ? 'border-slate-700/50 opacity-75' : 'border-slate-600 shadow-lg',
                      config.borderColor,
                    )}
                    style={{ animation: 'fadeInUp 0.3s ease-out' }}
                  >
                    <div className="flex items-start gap-4">
                      <label className="flex items-center mt-1">
                        <input
                          type="checkbox"
                          checked={selectedIds.has(alert.id)}
                          onChange={() => toggleSelect(alert.id)}
                          className="w-4 h-4 rounded border-slate-600 bg-slate-700 text-blue-600 focus:ring-blue-500 focus:ring-offset-0 cursor-pointer"
                        />
                      </label>
                      <div className={cn('p-2.5 rounded-lg flex-shrink-0', config.color)}>
                        <Icon className="w-5 h-5" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-start justify-between gap-2">
                          <div>
                            <div className="flex items-center gap-2 mb-1">
                              <h3 className={cn('text-sm font-semibold', alert.read ? 'text-slate-400' : 'text-white')}>
                                {alert.title}
                              </h3>
                              {!alert.read && (
                                <span className="w-2 h-2 bg-blue-500 rounded-full flex-shrink-0" />
                              )}
                            </div>
                            <p className="text-sm text-slate-400 leading-relaxed">{alert.message}</p>
                          </div>
                        </div>
                        <div className="flex items-center gap-3 mt-3">
                          <Badge variant={severity.variant} size="sm">
                            {severity.label}
                          </Badge>
                          <Badge>
                            {config.label}
                          </Badge>
                          <span className="text-xs text-slate-500">{alert.timestamp ? formatRelativeDate(alert.timestamp) : '--'}</span>
                          <div className="ml-auto flex items-center gap-2">
                            {!alert.read && (
                              <button
                                onClick={(e) => { e.stopPropagation(); markRead(alert.id); }}
                                className="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1"
                              >
                                <CheckCircleIcon className="w-3.5 h-3.5" />
                                Mark as read
                              </button>
                            )}
                            <button
                              onClick={(e) => { e.stopPropagation(); dismissAlert(alert.id); }}
                              className="text-xs text-slate-500 hover:text-slate-300 flex items-center gap-1"
                            >
                              <XMarkIcon className="w-3.5 h-3.5" />
                              Dismiss
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
        {filtered.length === 0 && (
          <div className="text-center py-12 text-slate-500">
            <BellAlertIcon className="w-12 h-12 mx-auto mb-3 opacity-50" />
            <p>No alerts match your filter</p>
          </div>
        )}
      </div>

      <style jsx>{`
        @keyframes fadeInUp {
          from { opacity: 0; transform: translateY(8px); }
          to { opacity: 1; transform: translateY(0); }
        }
      `}</style>
    </div>
  );
}
