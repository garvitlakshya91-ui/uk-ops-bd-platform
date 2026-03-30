'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import {
  HomeIcon,
  DocumentTextIcon,
  ViewColumnsIcon,
  BuildingOffice2Icon,
  BuildingLibraryIcon,
  BellAlertIcon,
  CpuChipIcon,
  DocumentDuplicateIcon,
  Bars3Icon,
  XMarkIcon,
  BellIcon,
  PlusIcon,
  ArrowRightOnRectangleIcon,
} from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils';
import { AuthProvider, useAuth } from '@/lib/auth';
import './globals.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 30000,
    },
  },
});

const navItems = [
  { href: '/dashboard', label: 'Dashboard', icon: HomeIcon },
  { href: '/applications', label: 'Applications', icon: DocumentTextIcon },
  { href: '/pipeline', label: 'Pipeline', icon: ViewColumnsIcon },
  { href: '/schemes', label: 'Schemes', icon: BuildingOffice2Icon },
  { href: '/contracts', label: 'Contracts', icon: DocumentDuplicateIcon },
  { href: '/companies', label: 'Companies', icon: BuildingLibraryIcon },
  { href: '/alerts', label: 'Alerts', icon: BellAlertIcon },
  { href: '/scraper-health', label: 'Scraper Health', icon: CpuChipIcon },
];

const ROLE_LABELS: Record<string, string> = {
  admin: 'Admin',
  bd_manager: 'BD Manager',
  bd_analyst: 'BD Analyst',
  viewer: 'Viewer',
};

const ROLE_COLORS: Record<string, string> = {
  admin: 'bg-red-500/20 text-red-400 border-red-500/30',
  bd_manager: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  bd_analyst: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
  viewer: 'bg-slate-500/20 text-slate-400 border-slate-500/30',
};

function getInitials(name: string): string {
  return name
    .split(' ')
    .map((n) => n[0])
    .join('')
    .toUpperCase()
    .slice(0, 2);
}

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

function formatTopBarDate(): string {
  return new Date().toLocaleDateString('en-GB', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });
}

function SidebarUser() {
  const { user, logout } = useAuth();

  if (!user) return null;

  const roleLabel = ROLE_LABELS[user.role] || user.role;
  const roleColor = ROLE_COLORS[user.role] || ROLE_COLORS.viewer;

  return (
    <div className="px-4 py-4 border-t border-slate-800/80">
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-purple-600 rounded-full flex items-center justify-center text-sm font-bold text-white shadow-lg shadow-blue-500/20">
          {getInitials(user.name)}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-white truncate">{user.name}</p>
          <span
            className={cn(
              'inline-block mt-0.5 text-[10px] font-semibold px-1.5 py-0.5 rounded border',
              roleColor
            )}
          >
            {roleLabel}
          </span>
        </div>
        <button
          onClick={logout}
          title="Sign out"
          className="p-1.5 text-slate-500 hover:text-red-400 rounded-lg hover:bg-slate-800 transition-colors"
        >
          <ArrowRightOnRectangleIcon className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}

function Sidebar({ mobile, onClose }: { mobile?: boolean; onClose?: () => void }) {
  const pathname = usePathname();

  return (
    <div
      className={cn(
        'flex flex-col h-full bg-slate-900 border-r border-slate-800/80',
        mobile ? 'w-64' : 'w-64 hidden lg:flex'
      )}
    >
      {/* Logo area */}
      <div className="flex items-center justify-between px-5 py-5 border-b border-slate-800/80">
        <div className="flex items-center gap-3">
          {/* Gradient brand mark */}
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 via-blue-600 to-purple-600 flex items-center justify-center shadow-lg shadow-blue-500/20">
            <span className="text-white font-bold text-sm">UK</span>
          </div>
          <div>
            <h1 className="text-base font-bold text-white tracking-tight">UK Ops BD</h1>
            <p className="text-[10px] text-slate-500 font-medium uppercase tracking-widest">Business Intelligence</p>
          </div>
        </div>
        {mobile && onClose && (
          <button onClick={onClose} className="p-1 text-slate-400 hover:text-white">
            <XMarkIcon className="w-5 h-5" />
          </button>
        )}
      </div>

      {/* Gradient separator */}
      <div className="h-[1px] bg-gradient-to-r from-transparent via-blue-500/30 to-transparent" />

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {navItems.map((item) => {
          const isActive = pathname === item.href || pathname.startsWith(item.href + '/');
          return (
            <Link
              key={item.href}
              href={item.href}
              onClick={onClose}
              className={cn('sidebar-link', isActive && 'active')}
            >
              <item.icon className="w-5 h-5 flex-shrink-0" />
              <span>{item.label}</span>
              {item.label === 'Alerts' && (
                <span className="ml-auto bg-red-500/20 text-red-400 text-xs font-bold px-2 py-0.5 rounded-full">
                  3
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      {/* Quick Actions */}
      <div className="px-3 pb-3">
        <p className="px-3 mb-2 text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Quick Actions</p>
        <button className="w-full flex items-center gap-2 px-3 py-2.5 rounded-lg text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 transition-all duration-200 shadow-lg shadow-blue-500/20">
          <PlusIcon className="w-4 h-4" />
          <span>Add Opportunity</span>
        </button>
      </div>

      {/* Pipeline stats separator */}
      <div className="h-[1px] bg-gradient-to-r from-transparent via-slate-700/50 to-transparent" />

      {/* Mini stats */}
      <div className="px-5 py-3 border-t border-slate-800/50">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-slate-500">Pipeline</span>
          <span className="text-blue-400 font-semibold">£14.2M</span>
        </div>
        <div className="flex items-center justify-between text-[11px] mt-1">
          <span className="text-slate-500">Active</span>
          <span className="text-emerald-400 font-semibold">64</span>
        </div>
      </div>

      {/* User */}
      <SidebarUser />
    </div>
  );
}

function TopBar({ onMenuClick }: { onMenuClick: () => void }) {
  const { user } = useAuth();
  const firstName = user?.name?.split(' ')[0] || '';

  return (
    <div className="h-14 bg-slate-900/95 backdrop-blur-sm border-b border-slate-800/80 flex items-center justify-between px-4 lg:px-6">
      <div className="flex items-center gap-4">
        <button
          onClick={onMenuClick}
          className="lg:hidden p-2 text-slate-400 hover:text-white rounded-lg hover:bg-slate-800"
        >
          <Bars3Icon className="w-5 h-5" />
        </button>
        <div className="hidden sm:block">
          <p className="text-sm font-medium text-white">
            {getGreeting()}, <span className="text-blue-400">{firstName}</span>
          </p>
          <p className="text-[11px] text-slate-500">{formatTopBarDate()}</p>
        </div>
      </div>

      <div className="flex-1 max-w-md mx-4">
        <div className="relative">
          <input
            type="text"
            placeholder="Search applications, companies, schemes..."
            className="w-full pl-4 pr-4 py-1.5 bg-slate-800/80 border border-slate-700/50 rounded-lg text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40 focus:border-blue-500/30 transition-all"
          />
        </div>
      </div>

      <div className="flex items-center gap-3">
        {/* Sync indicator */}
        <div className="hidden md:flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-800/60 border border-slate-700/30">
          <div className="w-2 h-2 bg-emerald-400 rounded-full pulse-glow-green" />
          <span className="text-[11px] text-slate-400">Last sync: 5 min ago</span>
        </div>

        {/* Notification bell with count */}
        <Link
          href="/alerts"
          className="relative p-2 text-slate-400 hover:text-white rounded-lg hover:bg-slate-800 transition-colors"
        >
          <BellIcon className="w-5 h-5" />
          <span className="absolute -top-0.5 -right-0.5 min-w-[18px] h-[18px] flex items-center justify-center bg-red-500 text-white text-[10px] font-bold rounded-full shadow-lg shadow-red-500/30">
            3
          </span>
        </Link>
      </div>
    </div>
  );
}

const PUBLIC_PATHS = ['/login', '/register'];

function AuthenticatedShell({ children }: { children: React.ReactNode }) {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const { user, loading } = useAuth();
  const pathname = usePathname();

  // On public pages, render children directly (login/register handle their own layout)
  if (PUBLIC_PATHS.includes(pathname)) {
    return <>{children}</>;
  }

  // Show loading spinner while checking auth
  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-900">
        <div className="flex flex-col items-center gap-3">
          <svg
            className="animate-spin h-8 w-8 text-blue-500"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          <p className="text-sm text-slate-500">Loading...</p>
        </div>
      </div>
    );
  }

  // If not authenticated, AuthProvider will redirect; render nothing while that happens
  if (!user) {
    return null;
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      {mobileMenuOpen && (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div
            className="fixed inset-0 bg-black/60 backdrop-blur-sm"
            onClick={() => setMobileMenuOpen(false)}
          />
          <div className="relative z-50">
            <Sidebar mobile onClose={() => setMobileMenuOpen(false)} />
          </div>
        </div>
      )}
      <div className="flex-1 flex flex-col overflow-hidden">
        <TopBar onMenuClick={() => setMobileMenuOpen(true)} />
        <main className="flex-1 overflow-y-auto p-4 lg:p-6">{children}</main>
      </div>
    </div>
  );
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="bg-slate-900">
        <QueryClientProvider client={queryClient}>
          <AuthProvider>
            <AuthenticatedShell>{children}</AuthenticatedShell>
          </AuthProvider>
          <Toaster
            position="top-right"
            toastOptions={{
              style: {
                background: '#1e293b',
                color: '#e2e8f0',
                border: '1px solid #334155',
                borderRadius: '12px',
              },
            }}
          />
        </QueryClientProvider>
      </body>
    </html>
  );
}
