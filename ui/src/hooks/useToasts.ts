// ─────────────────────────────────────────────────────────────────────────────
// useToasts — error (red) and success (green) toast notifications.
// Auto-clears after the configured delay so callers just call show*().
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from 'react';

export function useToasts() {
  const [cellError, setCellError] = useState<string | null>(null);
  const [retryStormId, setRetryStormId] = useState<string | null>(null);
  const [toastSuccess, setToastSuccess] = useState<string | null>(null);

  useEffect(() => {
    if (!cellError) return;
    const t = setTimeout(() => { setCellError(null); setRetryStormId(null); }, 8000);
    return () => clearTimeout(t);
  }, [cellError]);

  useEffect(() => {
    if (!toastSuccess) return;
    const t = setTimeout(() => setToastSuccess(null), 3000);
    return () => clearTimeout(t);
  }, [toastSuccess]);

  const showError = (msg: string, stormId?: string) => {
    setCellError(msg);
    if (stormId) setRetryStormId(stormId);
  };

  return { cellError, retryStormId, toastSuccess, setToastSuccess, showError };
}
