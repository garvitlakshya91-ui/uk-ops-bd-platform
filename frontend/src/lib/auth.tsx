'use client';

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import api from '@/lib/api';

export interface User {
  id: string;
  email: string;
  name: string;
  role: 'admin' | 'bd_manager' | 'bd_analyst' | 'viewer';
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string, name: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const PUBLIC_PATHS = ['/login', '/register'];

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();
  const pathname = usePathname();

  // On mount, validate existing token
  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    if (!token) {
      setLoading(false);
      return;
    }

    api
      .get('/auth/me')
      .then(({ data }) => {
        setUser(data);
      })
      .catch(() => {
        localStorage.removeItem('auth_token');
        localStorage.removeItem('refresh_token');
      })
      .finally(() => {
        setLoading(false);
      });
  }, []);

  // Auth guard: redirect to /login if not authenticated and not on a public path
  useEffect(() => {
    if (loading) return;
    if (!user && !PUBLIC_PATHS.includes(pathname)) {
      router.replace('/login');
    }
  }, [user, loading, pathname, router]);

  const login = useCallback(
    async (email: string, password: string) => {
      const { data } = await api.post('/auth/login', { email, password });
      localStorage.setItem('auth_token', data.access_token);
      localStorage.setItem('refresh_token', data.refresh_token);
      setUser(data.user);
      router.push('/dashboard');
    },
    [router]
  );

  const register = useCallback(
    async (email: string, password: string, name: string) => {
      await api.post('/auth/register', { email, password, name });
      // After registration, log the user in automatically
      await login(email, password);
    },
    [login]
  );

  const logout = useCallback(() => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('refresh_token');
    setUser(null);
    router.push('/login');
  }, [router]);

  return (
    <AuthContext.Provider value={{ user, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
