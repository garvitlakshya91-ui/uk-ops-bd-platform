'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { useAuth } from '@/lib/auth';

export default function LoginPage() {
  const { login } = useAuth();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await login(email, password);
    } catch (err: unknown) {
      const msg =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        'Invalid email or password';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-900 flex items-center justify-center px-4">
      <div className="w-full max-w-md">
        {/* Brand */}
        <div className="flex items-center justify-center gap-3 mb-8">
          <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-500 via-blue-600 to-purple-600 flex items-center justify-center shadow-lg shadow-blue-500/20">
            <span className="text-white font-bold text-base">UK</span>
          </div>
          <div>
            <h1 className="text-xl font-bold text-white tracking-tight">UK Ops BD</h1>
            <p className="text-[10px] text-slate-500 font-medium uppercase tracking-widest">
              Business Intelligence
            </p>
          </div>
        </div>

        {/* Card */}
        <div className="bg-slate-800/60 border border-slate-700/50 rounded-2xl p-8 shadow-xl">
          <h2 className="text-lg font-semibold text-white mb-1">Sign in to your account</h2>
          <p className="text-sm text-slate-400 mb-6">
            Enter your credentials to access the platform.
          </p>

          {error && (
            <div className="mb-4 px-4 py-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-slate-300 mb-1.5">
                Email
              </label>
              <input
                id="email"
                type="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@company.com"
                className="w-full px-4 py-2.5 bg-slate-900/80 border border-slate-700/50 rounded-lg text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40 focus:border-blue-500/30 transition-all"
              />
            </div>

            <div>
              <label
                htmlFor="password"
                className="block text-sm font-medium text-slate-300 mb-1.5"
              >
                Password
              </label>
              <input
                id="password"
                type="password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter your password"
                className="w-full px-4 py-2.5 bg-slate-900/80 border border-slate-700/50 rounded-lg text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40 focus:border-blue-500/30 transition-all"
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 transition-all duration-200 shadow-lg shadow-blue-500/20 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? (
                <>
                  <svg
                    className="animate-spin h-4 w-4 text-white"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                  >
                    <circle
                      className="opacity-25"
                      cx="12"
                      cy="12"
                      r="10"
                      stroke="currentColor"
                      strokeWidth="4"
                    />
                    <path
                      className="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                    />
                  </svg>
                  Signing in...
                </>
              ) : (
                'Sign in'
              )}
            </button>
          </form>
        </div>

        <p className="mt-6 text-center text-sm text-slate-500">
          First time?{' '}
          <Link
            href="/register"
            className="text-blue-400 hover:text-blue-300 font-medium transition-colors"
          >
            Create an account
          </Link>
        </p>
      </div>
    </div>
  );
}
