import { randomUUID, createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { Storage } from "@google-cloud/storage";
import SftpClient from "ssh2-sftp-client";
import { query, table } from "../db/index.js";
import { config } from "../config.js";

const storage = new Storage({ projectId: config.projectId });

// ── Types ──────────────────────────────────────────────────────────

export type SftpConnection = {
  connection_id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  remote_path: string;
  is_active: boolean;
  created_by: string;
  created_at: string;
  updated_at: string;
};

export type SftpUpload = {
  upload_id: string;
  report_id: string;
  connection_id: string;
  status: "pending" | "uploading" | "done" | "error";
  remote_file: string | null;
  error_message: string | null;
  initiated_by: string;
  created_at: string;
  completed_at: string | null;
};

export type CreateConnectionInput = {
  name: string;
  host: string;
  port: number;
  username: string;
  password: string;
  remotePath: string;
};

export type UpdateConnectionInput = Partial<CreateConnectionInput>;

// ── Encryption helpers (AES-256-GCM) ──────────────────────────────

function getEncryptionKey(): Buffer {
  const hex = config.sftpEncryptionKey;
  if (!hex || hex.length < 64) {
    throw new Error("SFTP_ENCRYPTION_KEY must be a 64-char hex string (32 bytes)");
  }
  return Buffer.from(hex, "hex");
}

export function encryptPassword(plain: string): string {
  const key = getEncryptionKey();
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const encrypted = Buffer.concat([cipher.update(plain, "utf8"), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return `${iv.toString("hex")}:${authTag.toString("hex")}:${encrypted.toString("hex")}`;
}

export function decryptPassword(encrypted: string): string {
  const key = getEncryptionKey();
  const [ivHex, authTagHex, dataHex] = encrypted.split(":");
  const iv = Buffer.from(ivHex, "hex");
  const authTag = Buffer.from(authTagHex, "hex");
  const data = Buffer.from(dataHex, "hex");
  const decipher = createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(authTag);
  return decipher.update(data) + decipher.final("utf8");
}

// ── CRUD ───────────────────────────────────────────────────────────

export async function listConnections(): Promise<SftpConnection[]> {
  return query<SftpConnection>(
    `SELECT connection_id, name, host, port, username, remote_path,
            is_active, created_by,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("sftp_connections")}
     WHERE is_active = true
     ORDER BY name`
  );
}

export async function getConnection(connectionId: string): Promise<(SftpConnection & { password_encrypted: string }) | null> {
  const rows = await query<SftpConnection & { password_encrypted: string }>(
    `SELECT connection_id, name, host, port, username, password_encrypted,
            remote_path, is_active, created_by,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("sftp_connections")}
     WHERE connection_id = @connectionId`,
    { connectionId }
  );
  return rows[0] ?? null;
}

export async function createConnection(
  input: CreateConnectionInput,
  userId: string
): Promise<{ connectionId: string }> {
  const connectionId = randomUUID();
  const passwordEncrypted = encryptPassword(input.password);

  await query(
    `INSERT INTO ${table("sftp_connections")} (
       connection_id, name, host, port, username, password_encrypted,
       remote_path, created_by, created_at, updated_at
     ) VALUES (
       @connectionId, @name, @host, @port, @username, @passwordEncrypted,
       @remotePath, @userId, NOW(), NOW()
     )`,
    {
      connectionId,
      name: input.name,
      host: input.host,
      port: input.port,
      username: input.username,
      passwordEncrypted,
      remotePath: input.remotePath,
      userId,
    }
  );

  return { connectionId };
}

export async function updateConnection(
  connectionId: string,
  input: UpdateConnectionInput
): Promise<void> {
  const sets: string[] = ["updated_at = NOW()"];
  const params: Record<string, unknown> = { connectionId };

  if (input.name !== undefined) {
    sets.push("name = @name");
    params.name = input.name;
  }
  if (input.host !== undefined) {
    sets.push("host = @host");
    params.host = input.host;
  }
  if (input.port !== undefined) {
    sets.push("port = @port");
    params.port = input.port;
  }
  if (input.username !== undefined) {
    sets.push("username = @username");
    params.username = input.username;
  }
  if (input.password !== undefined) {
    sets.push("password_encrypted = @passwordEncrypted");
    params.passwordEncrypted = encryptPassword(input.password);
  }
  if (input.remotePath !== undefined) {
    sets.push("remote_path = @remotePath");
    params.remotePath = input.remotePath;
  }

  await query(
    `UPDATE ${table("sftp_connections")} SET ${sets.join(", ")} WHERE connection_id = @connectionId`,
    params
  );
}

export async function deleteConnection(connectionId: string): Promise<void> {
  await query(
    `UPDATE ${table("sftp_connections")} SET is_active = false, updated_at = NOW() WHERE connection_id = @connectionId`,
    { connectionId }
  );
}

// ── Test Connection ────────────────────────────────────────────────

export async function testConnection(connectionId: string): Promise<{ success: boolean; message: string }> {
  const conn = await getConnection(connectionId);
  if (!conn) return { success: false, message: "Connection not found" };

  const sftp = new SftpClient();
  try {
    const password = decryptPassword(conn.password_encrypted);
    await sftp.connect({
      host: conn.host,
      port: conn.port,
      username: conn.username,
      password,
      readyTimeout: 10000,
    });
    // Verify remote path exists
    const exists = await sftp.exists(conn.remote_path);
    await sftp.end();

    if (exists) {
      return { success: true, message: `Connected successfully. Remote path "${conn.remote_path}" exists.` };
    }
    return { success: false, message: `Connected but remote path "${conn.remote_path}" does not exist.` };
  } catch (err) {
    try { await sftp.end(); } catch { /* ignore */ }
    const message = err instanceof Error ? err.message : String(err);
    return { success: false, message: `Connection failed: ${message}` };
  }
}

// ── Upload Report to SFTP ──────────────────────────────────────────

async function updateUploadStatus(
  uploadId: string,
  status: SftpUpload["status"],
  extra: Record<string, unknown> = {}
): Promise<void> {
  const sets = ["status = @status"];
  const params: Record<string, unknown> = { uploadId, status };

  if (extra.remoteFile !== undefined) {
    sets.push("remote_file = @remoteFile");
    params.remoteFile = extra.remoteFile;
  }
  if (extra.errorMessage !== undefined) {
    sets.push("error_message = @errorMessage");
    params.errorMessage = extra.errorMessage;
  }
  if (status === "done" || status === "error") {
    sets.push("completed_at = NOW()");
  }

  await query(
    `UPDATE ${table("sftp_uploads")} SET ${sets.join(", ")} WHERE upload_id = @uploadId`,
    params
  );
}

export async function uploadReportToSftp(
  reportId: string,
  connectionId: string,
  userId: string
): Promise<{ uploadId: string }> {
  const uploadId = randomUUID();

  await query(
    `INSERT INTO ${table("sftp_uploads")} (
       upload_id, report_id, connection_id, status, initiated_by, created_at
     ) VALUES (
       @uploadId, @reportId, @connectionId, 'pending', @userId, NOW()
     )`,
    { uploadId, reportId, connectionId, userId }
  );

  // Fire and forget
  doUpload(uploadId, reportId, connectionId).catch((err) =>
    console.error(`SFTP upload ${uploadId} failed:`, err)
  );

  return { uploadId };
}

async function doUpload(uploadId: string, reportId: string, connectionId: string): Promise<void> {
  try {
    await updateUploadStatus(uploadId, "uploading");

    // Get report details
    const reports = await query<{ file_url: string; report_name: string; status: string }>(
      `SELECT file_url, report_name, status FROM ${table("reports")} WHERE report_id = @reportId`,
      { reportId }
    );
    const report = reports[0];
    if (!report) throw new Error("Report not found");
    if (report.status !== "done" || !report.file_url) throw new Error("Report not ready for download");

    // Get connection details
    const conn = await getConnection(connectionId);
    if (!conn) throw new Error("SFTP connection not found");

    const password = decryptPassword(conn.password_encrypted);

    // Stream file from GCS
    const bucket = storage.bucket(config.reportsBucket);
    const file = bucket.file(report.file_url);
    const readStream = file.createReadStream();

    // Upload to SFTP
    const remotePath = conn.remote_path.endsWith("/") ? conn.remote_path : `${conn.remote_path}/`;
    const remoteFile = `${remotePath}${report.report_name}.csv`;

    const sftp = new SftpClient();
    await sftp.connect({
      host: conn.host,
      port: conn.port,
      username: conn.username,
      password,
      readyTimeout: 10000,
    });

    await sftp.put(readStream, remoteFile);
    await sftp.end();

    await updateUploadStatus(uploadId, "done", { remoteFile });
    console.log(`SFTP upload ${uploadId} completed: ${remoteFile}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`SFTP upload ${uploadId} failed:`, message);
    await updateUploadStatus(uploadId, "error", { errorMessage: message }).catch(() => {});
  }
}

// ── Upload History ─────────────────────────────────────────────────

export async function listUploads(reportId: string): Promise<SftpUpload[]> {
  return query<SftpUpload>(
    `SELECT u.upload_id, u.report_id, u.connection_id, u.status,
            u.remote_file, u.error_message, u.initiated_by,
            u.created_at::text AS created_at,
            u.completed_at::text AS completed_at
     FROM ${table("sftp_uploads")} u
     WHERE u.report_id = @reportId
     ORDER BY u.created_at DESC`,
    { reportId }
  );
}
