import PDFDocument from "pdfkit";
import type { SecurityTestRow } from "./platformHealthService.js";

/**
 * Generate a PDF security test report from a stored result row.
 * Returns a Buffer containing the complete PDF.
 */
export function generateSecurityReportPdf(row: SecurityTestRow): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 40 });
    const chunks: Buffer[] = [];
    doc.on("data", (chunk: Buffer) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    const statusLabel =
      row.status === "no_errors"
        ? "No Errors"
        : row.status === "critical_errors"
          ? "Critical Errors"
          : "Minor Errors";

    const typeLabel =
      row.test_type === "auth-security"
        ? "Auth & Password Security Test"
        : "Penetration Test";

    // ── Header ───────────────────────────────────────────────────────
    doc.fontSize(20).font("Helvetica-Bold").text("Security Test Report", { align: "center" });
    doc.moveDown(0.5);
    doc.fontSize(10).font("Helvetica").fillColor("#666").text("BeaconLab.CDX Platform Health", { align: "center" });
    doc.moveDown(1.5);

    // ── Meta ─────────────────────────────────────────────────────────
    doc.fillColor("#000").fontSize(12).font("Helvetica-Bold").text("Test Details");
    doc.moveDown(0.3);
    doc.fontSize(10).font("Helvetica");
    doc.text(`Type:        ${typeLabel}`);
    doc.text(`Date:        ${new Date(row.ran_at).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" })}`);
    doc.text(`Status:      ${statusLabel}`);
    doc.text(`Environment: ${row.environment}`);
    if (row.target_url) doc.text(`Target:      ${row.target_url}`);
    doc.moveDown(1);

    // ── Summary ──────────────────────────────────────────────────────
    doc.fontSize(12).font("Helvetica-Bold").text("Summary");
    doc.moveDown(0.3);
    doc.fontSize(10).font("Helvetica");
    doc.text(`Total Passed:  ${row.passed}`);
    doc.text(`Total Failed:  ${row.failed}`);
    doc.text(`  Critical:    ${row.critical_fails}`);
    doc.text(`  High:        ${row.high_fails}`);
    doc.text(`  Medium:      ${row.medium_fails}`);
    doc.text(`  Low:         ${row.low_fails}`);
    doc.moveDown(1);

    // ── Findings ─────────────────────────────────────────────────────
    const findings = row.findings_json as Array<{
      name: string;
      category: string;
      severity: string;
      details: string;
      evidence?: string;
      remediation?: string;
    }>;

    if (findings.length > 0) {
      doc.fontSize(12).font("Helvetica-Bold").text("Findings");
      doc.moveDown(0.5);

      for (const f of findings) {
        // Severity badge color
        const sevColor =
          f.severity === "critical" ? "#dc2626"
          : f.severity === "high" ? "#ea580c"
          : f.severity === "medium" ? "#ca8a04"
          : "#6b7280";

        doc.fontSize(10).font("Helvetica-Bold").fillColor(sevColor)
          .text(`[${f.severity.toUpperCase()}]`, { continued: true })
          .fillColor("#000").text(`  ${f.name}`);

        doc.font("Helvetica").fontSize(9);
        doc.text(`  Category: ${f.category}`);
        doc.text(`  Details: ${f.details}`);
        if (f.evidence) doc.text(`  Evidence: ${f.evidence}`);
        if (f.remediation) doc.text(`  Remediation: ${f.remediation}`);
        doc.moveDown(0.5);

        // Page break if near bottom
        if (doc.y > 700) doc.addPage();
      }
    } else {
      doc.fontSize(11).font("Helvetica").fillColor("#16a34a").text("No findings — all checks passed.");
      doc.fillColor("#000");
    }

    doc.moveDown(1);

    // ── Passed Checks ────────────────────────────────────────────────
    const passedChecks = row.passed_checks as string[];
    if (passedChecks.length > 0) {
      if (doc.y > 650) doc.addPage();
      doc.fontSize(12).font("Helvetica-Bold").fillColor("#000").text("Passed Checks");
      doc.moveDown(0.3);
      doc.fontSize(9).font("Helvetica");
      for (const name of passedChecks) {
        doc.fillColor("#16a34a").text("  \u2713 ", { continued: true }).fillColor("#000").text(name);
        if (doc.y > 750) doc.addPage();
      }
    }

    doc.end();
  });
}
