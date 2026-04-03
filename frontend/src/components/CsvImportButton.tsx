import { useRef, useState } from "react";

interface Props {
  token: string | null;
  disabled: boolean;
}

export default function CsvImportButton({ token, disabled }: Props) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [status, setStatus] = useState<string | null>(null);

  const handleImport = async (file: File) => {
    if (!token) return;

    const form = new FormData();
    form.append("file", file);

    try {
      const res = await fetch("/api/communities/import", {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
        body: form,
      });

      if (!res.ok) {
        const err = await res.json();
        setStatus(err.detail || "Import failed");
        return;
      }

      const data = await res.json();
      setStatus(`Imported ${data.imported} communities`);
    } catch {
      setStatus("Import failed — network error");
    }

    // Clear the file input so the same file can be re-uploaded
    if (fileRef.current) fileRef.current.value = "";

    // Auto-dismiss status after 4 seconds
    setTimeout(() => setStatus(null), 4000);
  };

  return (
    <span className="csv-import-wrapper">
      <button
        type="button"
        className="btn-secondary btn-sm"
        onClick={() => fileRef.current?.click()}
        disabled={disabled}
      >
        Import CSV
      </button>
      <input
        ref={fileRef}
        type="file"
        accept=".csv"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) handleImport(file);
        }}
      />
      {status && <span className="csv-import-status">{status}</span>}
    </span>
  );
}
