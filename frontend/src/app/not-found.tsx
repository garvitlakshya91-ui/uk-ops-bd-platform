import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="min-h-[60vh] flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="text-6xl font-bold text-slate-700">404</div>
        <h2 className="text-xl font-semibold text-white">Page not found</h2>
        <p className="text-sm text-slate-400 max-w-md">
          The page you are looking for does not exist or has been moved.
        </p>
        <Link
          href="/dashboard"
          className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 rounded-lg transition-all"
        >
          Go to Dashboard
        </Link>
      </div>
    </div>
  );
}
