const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');

const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const DATA_DIR = path.join(ROOT, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const sessions = new Map();

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function nowIso() {
  return new Date().toISOString();
}

function normalizeEmail(v) {
  return String(v || '').trim().toLowerCase();
}

function normalizeCode(v) {
  return String(v || '').trim().toUpperCase();
}

function safeText(v) {
  return String(v || '').trim();
}

function chooseLogoValue(body, existing = '') {
  return safeText(body.logoDataUrl) || safeText(body.logoUrl) || safeText(existing);
}

function json(res, status, data) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(data));
}

function redirect(res, location) {
  res.writeHead(302, { Location: location });
  res.end();
}

function sendHtml(res, status, html) {
  res.writeHead(status, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
}

function getMime(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  return {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.txt': 'text/plain; charset=utf-8'
  }[ext] || 'application/octet-stream';
}

function serveStatic(req, res, pathname) {
  let filePath = path.join(PUBLIC_DIR, pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, ''));
  if (!filePath.startsWith(PUBLIC_DIR)) return json(res, 403, { message: 'Forbidden' });
  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) return json(res, 404, { message: 'Not found' });
  res.writeHead(200, { 'Content-Type': getMime(filePath) });
  fs.createReadStream(filePath).pipe(res);
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', chunk => {
      raw += chunk;
      if (raw.length > 2 * 1024 * 1024) {
        reject(new Error('Payload too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error('Invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

function createEmptyDb() {
  return {
    meta: {
      initialized: false,
      created_at: nowIso(),
      nextIds: {
        schools: 1,
        users: 1,
        parents: 1,
        students: 1,
        teachers: 1,
        fees: 1,
        payments: 1,
        attendance: 1,
        results: 1,
        notices: 1,
        admissionRequests: 1,
        onboardingRequests: 1,
        resetRequests: 1,
        notifications: 1,
        salaries: 1,
        salaryPayments: 1
      }
    },
    schools: [],
    users: [],
    parents: [],
    students: [],
    teachers: [],
    fees: [],
    payments: [],
    attendance: [],
    results: [],
    notices: [],
    admissionRequests: [],
    onboardingRequests: [],
    resetRequests: [],
    notifications: [],
    salaries: [],
    salaryPayments: []
  };
}

function ensureDbShape(db) {
  if (!db.meta) db.meta = { initialized: false, created_at: nowIso(), nextIds: {} };
  if (!db.meta.nextIds) db.meta.nextIds = {};
  const defaults = {
    schools: [], users: [], parents: [], students: [], teachers: [], fees: [], payments: [],
    attendance: [], results: [], notices: [], admissionRequests: [], onboardingRequests: [],
    resetRequests: [], notifications: [], salaries: [], salaryPayments: []
  };
  Object.entries(defaults).forEach(([key, value]) => {
    if (!Array.isArray(db[key])) db[key] = value;
  });
  const nextIdDefaults = {
    schools: 1, users: 1, parents: 1, students: 1, teachers: 1, fees: 1, payments: 1,
    attendance: 1, results: 1, notices: 1, admissionRequests: 1, onboardingRequests: 1,
    resetRequests: 1, notifications: 1, salaries: 1, salaryPayments: 1
  };
  Object.entries(nextIdDefaults).forEach(([key, start]) => {
    if (!db.meta.nextIds[key]) {
      const max = Array.isArray(db[key]) && db[key].length ? Math.max(...db[key].map(x => Number(x.id) || 0)) + 1 : start;
      db.meta.nextIds[key] = max;
    }
  });
  return db;
}

function readDb() {
  if (!fs.existsSync(DB_FILE)) {
    const db = createEmptyDb();
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
    return db;
  }
  const db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  return ensureDbShape(db);
}

function writeDb(db) {
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
}

function nextId(db, key) {
  const current = db.meta.nextIds[key] || 1;
  db.meta.nextIds[key] = current + 1;
  return current;
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
  const text = String(stored || '');
  if (!text.includes(':')) return String(password || '') === text;
  const [salt, expected] = text.split(':');
  const actual = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
  const a = Buffer.from(actual, 'hex');
  const b = Buffer.from(expected, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function sanitizeUser(db, user) {
  if (!user) return null;
  const school = user.school_id ? db.schools.find(s => s.id === user.school_id) : null;
  return {
    id: user.id,
    role: user.role,
    name: user.name,
    email: user.email,
    school: school ? {
      id: school.id,
      name: school.name,
      code: school.code,
      tagline: school.tagline || '',
      logo_url: school.logo_url || '',
      primary_color: school.primary_color || '#2146d0',
      phone: school.phone || '',
      address: school.address || ''
    } : null
  };
}

function requireAuth(req, res, roles) {
  const auth = String(req.headers.authorization || '');
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token || !sessions.has(token)) {
    json(res, 401, { message: 'Please log in first.' });
    return null;
  }
  const session = sessions.get(token);
  const db = readDb();
  const user = db.users.find(u => u.id === session.user_id && u.is_active !== false);
  if (!user) {
    sessions.delete(token);
    json(res, 401, { message: 'Session expired.' });
    return null;
  }
  if (roles && !roles.includes(user.role)) {
    json(res, 403, { message: 'You are not allowed to access this section.' });
    return null;
  }
  return { db, user, token };
}

function schoolByCode(db, code) {
  return db.schools.find(s => s.code === normalizeCode(code));
}

function getSchoolScoped(db, schoolId) {
  return {
    school: db.schools.find(s => s.id === schoolId) || null,
    users: db.users.filter(x => x.school_id === schoolId),
    parents: db.parents.filter(x => x.school_id === schoolId),
    students: db.students.filter(x => x.school_id === schoolId),
    teachers: db.teachers.filter(x => x.school_id === schoolId),
    fees: db.fees.filter(x => x.school_id === schoolId),
    payments: db.payments.filter(x => x.school_id === schoolId),
    attendance: db.attendance.filter(x => x.school_id === schoolId),
    results: db.results.filter(x => x.school_id === schoolId),
    notices: db.notices.filter(x => x.school_id === schoolId),
    admissionRequests: db.admissionRequests.filter(x => x.school_id === schoolId),
    notifications: db.notifications.filter(x => x.school_id === schoolId),
    salaries: db.salaries.filter(x => x.school_id === schoolId),
    salaryPayments: db.salaryPayments.filter(x => x.school_id === schoolId)
  };
}

function computeGrade(obtained, total) {
  const pct = total > 0 ? (Number(obtained) / Number(total)) * 100 : 0;
  if (pct >= 90) return 'A+';
  if (pct >= 80) return 'A';
  if (pct >= 70) return 'B';
  if (pct >= 60) return 'C';
  if (pct >= 50) return 'D';
  return 'F';
}

function createToken() {
  return crypto.randomBytes(32).toString('hex');
}

function sum(list, key) {
  return list.reduce((n, item) => n + Number(item[key] || 0), 0);
}

function feeWithRelations(db, fee) {
  const student = db.students.find(s => s.id === fee.student_id) || null;
  const parent = student ? db.parents.find(p => p.id === student.parent_id) : null;
  const derived = deriveFeeState(db, fee);
  return {
    ...fee,
    ...derived,
    student_name: student?.name || '',
    class_name: student?.class_name || '',
    section: student?.section || '',
    parent_name: parent?.name || '',
    parent_phone: parent?.phone || ''
  };
}

function paymentWithRelations(db, payment) {
  const fee = db.fees.find(f => f.id === payment.fee_id) || null;
  const student = fee ? db.students.find(s => s.id === fee.student_id) : null;
  return {
    ...payment,
    fee_title: fee?.title || '',
    student_name: student?.name || ''
  };
}

function paidPaymentsForFee(db, feeId) {
  return db.payments.filter(p => p.fee_id === feeId && p.status === 'paid');
}

function deriveFeeState(db, fee) {
  const original = Number(fee.original_amount ?? fee.amount ?? 0);
  const paid = Number(fee.amount_paid ?? paidPaymentsForFee(db, fee.id).reduce((n, p) => n + Number(p.amount || 0), 0));
  const balance = Math.max(0, Number(fee.balance_due ?? (original - paid)));
  const status = balance <= 0 ? 'paid' : (paid > 0 ? 'partial' : (fee.status || 'unpaid'));
  return {
    original_amount: original,
    amount_paid: paid,
    balance_due: balance,
    status
  };
}

function paidSalaryPaymentsForSalary(db, salaryId) {
  return db.salaryPayments.filter(p => p.salary_id === salaryId && p.status === 'paid');
}

function deriveSalaryState(db, salary) {
  const gross = Number(salary.gross_amount ?? salary.amount ?? 0);
  const bonus = Number(salary.bonus_amount ?? salary.bonus ?? 0);
  const deduction = Number(salary.deduction_amount ?? salary.deduction ?? 0);
  const net = Math.max(0, Number(salary.net_amount ?? (gross + bonus - deduction)));
  const paid = Number(salary.amount_paid ?? paidSalaryPaymentsForSalary(db, salary.id).reduce((n, p) => n + Number(p.amount || 0), 0));
  const balance = Math.max(0, Number(salary.balance_due ?? (net - paid)));
  const status = balance <= 0 ? 'paid' : (paid > 0 ? 'partial' : (salary.status || 'unpaid'));
  return {
    gross_amount: gross,
    bonus_amount: bonus,
    deduction_amount: deduction,
    net_amount: net,
    amount_paid: paid,
    balance_due: balance,
    status
  };
}

function getTeacherSalaryTotals(db, teacherId) {
  const items = db.salaries.filter(s => s.teacher_id === teacherId);
  return items.reduce((acc, salary) => {
    const state = deriveSalaryState(db, salary);
    acc.total += Number(state.net_amount || 0);
    acc.paid += Number(state.amount_paid || 0);
    acc.pending += Number(state.balance_due || 0);
    return acc;
  }, { total: 0, paid: 0, pending: 0 });
}

function getPayrollSummary(db, schoolId) {
  const rows = db.salaries.filter(s => !schoolId || s.school_id === schoolId);
  const monthMap = new Map();
  const teacherYearMap = new Map();
  rows.forEach((salary) => {
    const state = deriveSalaryState(db, salary);
    const teacher = db.teachers.find(t => t.id === salary.teacher_id) || {};
    const month = safeText(salary.month_label) || 'Unspecified';
    if (!monthMap.has(month)) monthMap.set(month, { month_label: month, teachers: 0, gross_amount: 0, bonus_amount: 0, deduction_amount: 0, net_amount: 0, paid_amount: 0, balance_due: 0 });
    const monthRow = monthMap.get(month);
    monthRow.teachers += 1;
    monthRow.gross_amount += Number(state.gross_amount || 0);
    monthRow.bonus_amount += Number(state.bonus_amount || 0);
    monthRow.deduction_amount += Number(state.deduction_amount || 0);
    monthRow.net_amount += Number(state.net_amount || 0);
    monthRow.paid_amount += Number(state.amount_paid || 0);
    monthRow.balance_due += Number(state.balance_due || 0);

    const yearMatch = month.match(/(20\d{2})/);
    const year = yearMatch ? yearMatch[1] : 'Other';
    const key = `${teacher.id || 0}__${year}`;
    if (!teacherYearMap.has(key)) teacherYearMap.set(key, { teacher_id: teacher.id || 0, teacher_name: teacher.name || 'Unknown Teacher', subject: teacher.subject || '', year, months: 0, gross_amount: 0, bonus_amount: 0, deduction_amount: 0, net_amount: 0, paid_amount: 0, balance_due: 0 });
    const t = teacherYearMap.get(key);
    t.months += 1;
    t.gross_amount += Number(state.gross_amount || 0);
    t.bonus_amount += Number(state.bonus_amount || 0);
    t.deduction_amount += Number(state.deduction_amount || 0);
    t.net_amount += Number(state.net_amount || 0);
    t.paid_amount += Number(state.amount_paid || 0);
    t.balance_due += Number(state.balance_due || 0);
  });
  return {
    months: [...monthMap.values()].sort((a, b) => String(b.month_label).localeCompare(String(a.month_label))),
    teachers: [...teacherYearMap.values()].sort((a, b) => String(a.teacher_name).localeCompare(String(b.teacher_name)) || String(b.year).localeCompare(String(a.year)))
  };
}

function salaryWithRelations(db, salary) {
  const teacher = db.teachers.find(t => t.id === salary.teacher_id);
  const state = deriveSalaryState(db, salary);
  return {
    ...salary,
    ...state,
    teacher_name: teacher?.name || '',
    teacher_phone: teacher?.phone || '',
    teacher_email: teacher?.email || '',
    subject: teacher?.subject || '',
    assigned_class: teacher?.assigned_class || ''
  };
}

function getStudentFeeTotals(db, studentId) {
  const studentFees = db.fees.filter(f => f.student_id === studentId);
  return studentFees.reduce((acc, fee) => {
    const state = deriveFeeState(db, fee);
    acc.total += Number(state.original_amount || 0);
    acc.paid += Number(state.amount_paid || 0);
    acc.pending += Number(state.balance_due || 0);
    return acc;
  }, { total: 0, paid: 0, pending: 0 });
}

function getStudentAttendanceSummary(db, studentId) {
  return db.attendance.filter(a => a.student_id === studentId).reduce((acc, row) => {
    const key = String(row.status || '').toLowerCase();
    if (key === 'present') acc.present += 1;
    else if (key === 'absent') acc.absent += 1;
    else if (key === 'late') acc.late += 1;
    acc.total += 1;
    return acc;
  }, { total: 0, present: 0, absent: 0, late: 0 });
}

function getParentSummary(db, parentId, schoolId) {
  const children = db.students.filter(s => s.parent_id === parentId && (!schoolId || s.school_id === schoolId));
  const totals = children.reduce((acc, child) => {
    const childTotals = getStudentFeeTotals(db, child.id);
    acc.total += childTotals.total;
    acc.paid += childTotals.paid;
    acc.pending += childTotals.pending;
    return acc;
  }, { total: 0, paid: 0, pending: 0 });
  return {
    children_count: children.length,
    children_names: children.map(c => c.name).join(', '),
    total_fee: totals.total,
    total_paid: totals.paid,
    total_pending_fee: totals.pending
  };
}

function findPortalUserByLinked(db, role, linkedField, linkedId, schoolId) {
  return db.users.find(u => u.role === role && u[linkedField] === linkedId && u.school_id === schoolId) || null;
}

function inferTermKey(examTitle) {
  const text = String(examTitle || '').toLowerCase();
  if (/1st|first|term\s*1|quarter\s*1|q1/.test(text)) return 'term1';
  if (/2nd|second|term\s*2|quarter\s*2|q2/.test(text)) return 'term2';
  if (/3rd|third|term\s*3|quarter\s*3|q3/.test(text)) return 'term3';
  if (/4th|fourth|final|annual|term\s*4|quarter\s*4|q4/.test(text)) return 'term4';
  return 'obtained';
}

function generatePortalIdentity(prefix, schoolCode, id) {
  const safePrefix = String(prefix || 'user').replace(/[^a-z0-9]+/gi, '').toLowerCase() || 'user';
  const safeCode = String(schoolCode || 'school').replace(/[^a-z0-9]+/gi, '').toLowerCase() || 'school';
  const password = `${safePrefix.charAt(0).toUpperCase()}${safePrefix.slice(1)}@${1000 + Number(id || 1)}`;
  return {
    email: `${safePrefix}${id}@${safeCode}.local`,
    password
  };
}

function resolveSchool(db, schoolCode, schoolName) {
  const code = normalizeCode(schoolCode);
  if (code) return schoolByCode(db, code) || null;
  const name = safeText(schoolName).toLowerCase();
  if (!name) return null;
  return db.schools.find(s => safeText(s.name).toLowerCase() === name || normalizeCode(s.code) === normalizeCode(schoolName)) || null;
}

function buildReceiptUrl(paymentId) {
  return `/receipt/${paymentId}`;
}

function buildChallanUrl(feeId) {
  return `/challan/${feeId}`;
}

function buildResultCardUrl(studentId, examTitle) {
  return `/result-card/${studentId}?exam=${encodeURIComponent(String(examTitle || 'Exam Result'))}`;
}

function buildSalarySlipUrl(salaryId) {
  return `/salary-slip/${salaryId}`;
}

function buildPayrollSheetUrl(monthLabel) {
  return `/payroll-sheet?month=${encodeURIComponent(String(monthLabel || ''))}`;
}

function getBaseUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || 'http';
  const host = req.headers.host || `localhost:${PORT}`;
  return `${proto}://${host}`;
}

function absoluteUrl(req, relativeUrl) {
  if (!relativeUrl) return '';
  if (/^https?:\/\//i.test(relativeUrl)) return relativeUrl;
  return `${getBaseUrl(req)}${relativeUrl}`;
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildFeeShareLinks(req, fee, parentPhone, studentName) {
  const challanUrl = absoluteUrl(req, buildChallanUrl(fee.id));
  const phone = String(parentPhone || '').replace(/\D/g, '');
  const message = `Hello. The fee challan for ${studentName || 'the student'} is ready. Fee title: ${fee.title || ''}, balance due: Rs. ${Number((fee.balance_due ?? fee.amount) || 0).toLocaleString()}, due date: ${fee.due_date || ''}. View challan: ${challanUrl}`;
  return {
    challan_url: buildChallanUrl(fee.id),
    challan_abs_url: challanUrl,
    wa_link: phone ? `https://wa.me/${phone}?text=${encodeURIComponent(message)}` : '',
    sms_link: phone ? `sms:${phone}?body=${encodeURIComponent(message)}` : ''
  };
}

function renderPrintStyles(primaryColor = '#2146d0') {
  const color = escapeHtml(primaryColor || '#2146d0');
  return `
  <style>
    :root{--brand:${color};--ink:#0f172a;--muted:#64748b;--line:#dbe4f0;--soft:#f8fbff;--success:#166534}
    *{box-sizing:border-box}
    body{font-family:Arial,sans-serif;background:#eef3f9;padding:24px;color:var(--ink);margin:0}
    .sheet{position:relative;max-width:980px;margin:0 auto;background:#fff;border:1px solid #d9e2ef;border-radius:22px;padding:30px;box-shadow:0 22px 60px rgba(15,23,42,.08);overflow:hidden}
    .watermark{position:absolute;inset:0;display:grid;place-items:center;pointer-events:none;opacity:.06;font-size:88px;font-weight:800;letter-spacing:.18em;color:var(--brand);transform:rotate(-28deg)}
    .head{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;border-bottom:2px solid #edf3ff;padding-bottom:18px;margin-bottom:18px;position:relative;z-index:1}
    .brand{display:flex;gap:14px;align-items:center}.logo{width:62px;height:62px;border-radius:16px;object-fit:cover;border:1px solid #e5e7eb;background:#fff}
    .org{display:flex;flex-direction:column;gap:5px}.org h2{margin:0;font-size:28px;color:#0f1f3d}.org small{color:var(--muted);font-size:13px}
    .doc-title{text-align:right}.doc-title h3{margin:0;color:#0f1f3d;font-size:24px}.doc-title .mini{color:var(--muted);font-size:13px;line-height:1.6}
    .meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin:18px 0 14px;position:relative;z-index:1}
    .box{padding:14px 16px;border-radius:16px;background:linear-gradient(180deg,#ffffff 0%,#f8fbff 100%);border:1px solid #e4ebf5}
    .box strong{display:block;margin-bottom:6px;color:#0f1f3d}.box div{line-height:1.6;color:#334155}
    .summary{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:4px 0 18px;position:relative;z-index:1}
    .summary .item{padding:14px;border-radius:16px;background:#f8fbff;border:1px solid #e4ebf5}.summary b{display:block;font-size:13px;margin-bottom:6px;color:#475569}.summary span{font-size:24px;font-weight:800;color:#0f1f3d}
    table{width:100%;border-collapse:separate;border-spacing:0;margin-top:14px;position:relative;z-index:1}th,td{border-bottom:1px solid #e5eaf7;padding:12px 13px;text-align:left;font-size:14px}th{background:#f5f8fe;color:#334155;font-size:12px;text-transform:uppercase;letter-spacing:.05em;border-top:1px solid #e5eaf7}.table-shell{border:1px solid #e5eaf7;border-radius:18px;overflow:hidden;background:#fff}
    .status{display:inline-block;padding:6px 12px;border-radius:999px;background:#ecfdf3;border:1px solid #b7ebc8;color:var(--success);font-size:12px;font-weight:700}
    .copies{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;position:relative;z-index:1}
    .copy{border:1px dashed #c9d7eb;border-radius:20px;padding:18px;background:#fff}
    .copy h4{margin:0 0 8px;font-size:16px;color:#0f1f3d}
    .copy p{margin:4px 0;color:#475569;font-size:13px;line-height:1.6}
    .actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:22px;position:relative;z-index:1}.actions a,.actions button{appearance:none;background:linear-gradient(180deg,var(--brand) 0%,#18379e 100%);color:#fff;border:none;border-radius:12px;padding:11px 16px;font-weight:700;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center}.actions .light{background:#fff;color:var(--brand);border:1px solid #d5e0f5}
    .foot{margin-top:20px;font-size:13px;color:#475569;line-height:1.7;position:relative;z-index:1}
    .marks-row{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:0}
    .result-banner{display:flex;justify-content:space-between;gap:16px;padding:16px 18px;border:1px solid #e5eaf7;border-radius:18px;background:linear-gradient(180deg,#ffffff 0%,#f8fbff 100%);margin-bottom:16px;position:relative;z-index:1}
    .result-banner strong{display:block;color:#475569;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}.result-banner span{font-size:24px;font-weight:800;color:#0f1f3d}
    @media print { body{background:#fff;padding:0}.sheet{box-shadow:none;border:none;max-width:none;padding:0;border-radius:0} .actions{display:none} }
    @media (max-width:900px){.meta,.summary,.copies{grid-template-columns:1fr}.head{flex-direction:column}.result-banner{flex-direction:column}}
  </style>`;
}

function renderReceiptHtml(db, payment) {
  const fee = db.fees.find(f => f.id === payment.fee_id);
  const school = fee ? db.schools.find(s => s.id === fee.school_id) : null;
  const student = fee ? db.students.find(s => s.id === fee.student_id) : null;
  const parent = student ? db.parents.find(p => p.id === student.parent_id) : null;
  const schoolName = school?.name || 'School';
  return `<!doctype html><html><head><meta charset="utf-8"><title>Receipt ${payment.id}</title>${renderPrintStyles(school?.primary_color)}</head><body>
    <div class="sheet">
      <div class="watermark">${escapeHtml(schoolName)}</div>
      <div class="head">
        <div class="brand">
          ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : ''}
          <div class="org">
            <h2>${escapeHtml(schoolName)}</h2>
            <small>${escapeHtml(school?.tagline || '')}</small>
            <small>${escapeHtml(school?.address || '')}</small>
          </div>
        </div>
        <div class="doc-title">
          <h3>Official Fee Receipt</h3>
          <div class="mini">Receipt ID: ${payment.id}<br>Date: ${escapeHtml(String(payment.created_at || '').slice(0, 10))}</div>
        </div>
      </div>
      <div class="meta">
        <div class="box"><strong>Student</strong><div>${escapeHtml(student?.name || '')}</div><div>Class ${escapeHtml(student?.class_name || '')} ${escapeHtml(student?.section || '')}</div></div>
        <div class="box"><strong>Parent</strong><div>${escapeHtml(parent?.name || '')}</div><div>${escapeHtml(parent?.phone || '')}</div></div>
        <div class="box"><strong>Fee Title</strong><div>${escapeHtml(fee?.title || '')}</div><div>${escapeHtml(fee?.month_label || '')}</div></div>
        <div class="box"><strong>Payment Details</strong><div>Method: ${escapeHtml(payment.method || '')}</div><div>Ref: ${escapeHtml(payment.reference_no || '')}</div></div>
      </div>
      <div class="summary">
        <div class="item"><b>Paid Amount</b><span>Rs. ${Number(payment.amount || 0).toLocaleString()}</span></div>
        <div class="item"><b>Status</b><span style="font-size:18px">${escapeHtml(payment.status || 'paid')}</span></div>
        <div class="item"><b>Verification</b><span style="font-size:18px">Successful</span></div>
      </div>
      <div class="table-shell">
        <table>
          <tr><th>Description</th><th>Session / Month</th><th>Amount</th><th>Status</th></tr>
          <tr><td>${escapeHtml(fee?.title || 'Fee Payment')}</td><td>${escapeHtml(fee?.month_label || '')}</td><td>Rs. ${Number(payment.amount || 0).toLocaleString()}</td><td><span class="status">${escapeHtml(payment.status || 'paid')}</span></td></tr>
        </table>
      </div>
      <div class="foot">This is a system generated receipt for school records, parent record and audit trail.</div>
      <div class="actions"><button onclick="window.print()">Print Receipt</button></div>
    </div>
  </body></html>`;
}

function renderChallanHtml(db, fee, req) {
  const school = db.schools.find(s => s.id === fee.school_id);
  const derived = deriveFeeState(db, fee);
  const student = db.students.find(s => s.id === fee.student_id);
  const parent = student ? db.parents.find(p => p.id === student.parent_id) : null;
  const schoolName = school?.name || 'School';
  const challanAbs = absoluteUrl(req, buildChallanUrl(fee.id));
  const parentPhone = String(parent?.phone || '').replace(/\D/g, '');
  const message = `Hello. The fee challan for ${student?.name || 'the student'} is ready. Remaining balance: Rs. ${Number(derived.balance_due || 0).toLocaleString()}. Due date: ${fee.due_date || ''}. View challan: ${challanAbs}`;
  const waLink = parentPhone ? `https://wa.me/${parentPhone}?text=${encodeURIComponent(message)}` : '';
  const smsLink = parentPhone ? `sms:${parentPhone}?body=${encodeURIComponent(message)}` : '';

  return `<!doctype html><html><head><meta charset="utf-8"><title>Fee Challan ${fee.id}</title>
  ${renderPrintStyles(school?.primary_color)}
  <style>
    @page { size: A4 landscape; margin: 7mm; }
    body{background:#eef3f9;padding:12px}
    .sheet.challan-sheet{max-width:1120px;padding:7px 9px;border-radius:14px}
    .challan-sheet .watermark{font-size:74px;opacity:.035;transform:rotate(-24deg)}
    .challan-shell{position:relative;z-index:1;border:2px solid #0f1f3d;border-radius:18px;overflow:hidden;background:#fff}
    .challan-top{display:grid;grid-template-columns:1.4fr .9fr}
    .brand-side{padding:10px 12px;border-right:2px solid #0f1f3d;display:flex;gap:10px;align-items:flex-start}
    .brand-side .org h1{margin:0;font-size:24px;line-height:1.08;color:#0f1f3d}
    .brand-side .sub{font-size:12px;color:#475569;line-height:1.45;margin-top:4px}
    .meta-side{padding:10px 12px;background:linear-gradient(180deg,#f8fbff 0%,#edf4ff 100%)}
    .meta-side h2{margin:0 0 10px;font-size:22px;color:#0f1f3d;text-transform:uppercase;letter-spacing:.05em}
    .meta-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
    .meta-box{border:1px solid #d5e0f2;border-radius:12px;padding:7px 9px;background:#fff}
    .meta-box b{display:block;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#64748b;margin-bottom:3px}
    .meta-box span{display:block;font-size:13px;font-weight:700;color:#0f1f3d}
    .section{padding:0}
    .section-title{padding:8px 12px;background:#0f1f3d;color:#fff;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.08em}
    .grid-main{display:grid;grid-template-columns:1.2fr .8fr}
    .grid-main > div + div{border-left:2px solid #0f1f3d}
    table.slim{width:100%;border-collapse:collapse}
    table.slim td{border-bottom:1px solid #dbe4f2;padding:7px 9px;font-size:12px;vertical-align:top}
    table.slim td:first-child{width:36%;font-weight:800;color:#334155;background:#fbfdff}
    table.slim td:last-child{font-weight:600;color:#0f172a}
    .summary-strip{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;padding:8px 10px;border-top:2px solid #0f1f3d;border-bottom:2px solid #0f1f3d;background:#f9fbff}
    .sum-box{border:1px solid #d7e0f0;border-radius:12px;padding:7px 9px;background:#fff}
    .sum-box b{display:block;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#64748b;margin-bottom:4px}
    .sum-box span{display:block;font-size:20px;font-weight:900;color:#0f1f3d}
    .sum-box.balance{border:2px solid #2146d0;background:#eef4ff}
    .sum-box.balance span{color:#1638b6}
    .bottom-zone{display:grid;grid-template-columns:1.05fr .95fr}
    .bottom-zone > div + div{border-left:2px solid #0f1f3d}
    .notes{padding:8px 10px}
    .notes ul{margin:8px 0 0;padding-left:18px;color:#475569;font-size:11px;line-height:1.5}
    .sign-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:8px 10px}
    .sign{border:1px dashed #99adc8;border-radius:10px;min-height:48px;padding:7px 9px;background:#fff;color:#64748b;font-size:11px}
    .sign b{display:block;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#0f1f3d;margin-bottom:4px}
    .actions{position:relative;z-index:1}
    @media print{
      body{background:#fff;padding:0}
      .sheet.challan-sheet{box-shadow:none;border:none;max-width:none;padding:0;border-radius:0}
      .actions{display:none}
    }
    @media (max-width:920px){
      .challan-top,.grid-main,.summary-strip,.bottom-zone,.meta-grid,.sign-grid{grid-template-columns:1fr}
      .brand-side,.grid-main > div + div,.bottom-zone > div + div{border-right:none;border-left:none;border-top:2px solid #0f1f3d}
    }
  </style>
  </head><body>
    <div class="sheet challan-sheet">
      <div class="watermark">${escapeHtml(schoolName)}</div>
      <div class="challan-shell">
        <div class="challan-top">
          <div class="brand-side brand">
            ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : ''}
            <div class="org">
              <h1>${escapeHtml(schoolName)}</h1>
              <div class="sub">${escapeHtml(school?.tagline || '')}</div>
              <div class="sub">${escapeHtml(school?.address || '')}</div>
              <div class="sub">Phone: ${escapeHtml(school?.phone || '')}${school?.email ? ` | ${escapeHtml(school.email)}` : ''}</div>
            </div>
          </div>
          <div class="meta-side">
            <h2>Fee Challan</h2>
            <div class="meta-grid">
              <div class="meta-box"><b>Fee ID</b><span>${fee.id}</span></div>
              <div class="meta-box"><b>Status</b><span>${escapeHtml(String(derived.status || 'unpaid').toUpperCase())}</span></div>
              <div class="meta-box"><b>Issue Date</b><span>${escapeHtml(String(fee.created_at || nowIso()).slice(0,10))}</span></div>
              <div class="meta-box"><b>Due Date</b><span>${escapeHtml(fee.due_date || '')}</span></div>
            </div>
          </div>
        </div>

        <div class="grid-main">
          <div class="section">
            <div class="section-title">Student Details</div>
            <table class="slim">
              <tr><td>Student Name</td><td>${escapeHtml(student?.name || '')}</td></tr>
              <tr><td>Roll Number</td><td>${escapeHtml(student?.roll_no || '')}</td></tr>
              <tr><td>Class / Section</td><td>${escapeHtml(student?.class_name || '')} ${escapeHtml(student?.section || '')}</td></tr>
              <tr><td>Parent / Guardian</td><td>${escapeHtml(parent?.name || '')}</td></tr>
              <tr><td>Contact Number</td><td>${escapeHtml(parent?.phone || student?.phone || '')}</td></tr>
              <tr><td>Address</td><td>${escapeHtml(student?.address || parent?.address || '')}</td></tr>
            </table>
          </div>
          <div class="section">
            <div class="section-title">Fee Details</div>
            <table class="slim">
              <tr><td>Fee Title</td><td>${escapeHtml(fee.title || '')}</td></tr>
              <tr><td>Month / Session</td><td>${escapeHtml(fee.month_label || '')}</td></tr>
              <tr><td>Total Fee</td><td>Rs. ${Number(derived.original_amount || 0).toLocaleString()}</td></tr>
              <tr><td>Paid Before</td><td>Rs. ${Number(derived.amount_paid || 0).toLocaleString()}</td></tr>
              <tr><td>Remaining Fee</td><td><strong>Rs. ${Number(derived.balance_due || 0).toLocaleString()}</strong></td></tr>
              <tr><td>Remarks</td><td>${escapeHtml(fee.notes || '')}</td></tr>
            </table>
          </div>
        </div>

        <div class="summary-strip">
          <div class="sum-box"><b>Total Fee</b><span>Rs. ${Number(derived.original_amount || 0).toLocaleString()}</span></div>
          <div class="sum-box"><b>Paid So Far</b><span>Rs. ${Number(derived.amount_paid || 0).toLocaleString()}</span></div>
          <div class="sum-box balance"><b>Remaining Balance</b><span>Rs. ${Number(derived.balance_due || 0).toLocaleString()}</span></div>
        </div>

        <div class="bottom-zone">
          <div>
            <div class="section-title">Instructions</div>
            <div class="notes">
              <ul>
                <li>Use the same Fee ID for every partial payment until the remaining balance becomes zero.</li>
                <li>Bring this challan or open the digital version when making payment.</li>
                <li>After payment, the remaining balance will automatically update in the system.</li>
                <li>This challan is optimized to fit on one A4 landscape page.</li>
              </ul>
            </div>
          </div>
          <div>
            <div class="section-title">Office Verification</div>
            <div class="sign-grid">
              <div class="sign"><b>Received Amount</b></div>
              <div class="sign"><b>Reference Number</b></div>
              <div class="sign"><b>Accounts Stamp</b></div>
              <div class="sign"><b>Authorized Signature</b></div>
            </div>
          </div>
        </div>
      </div>

      <div class="actions">
        <button onclick="window.print()">Print Challan</button>
        ${waLink ? `<a href="${waLink}" target="_blank" rel="noopener">Send by WhatsApp</a>` : ''}
        ${smsLink ? `<a href="${smsLink}" class="light">Send by SMS</a>` : ''}
      </div>
    </div>
  </body></html>`;
}

function renderSalarySlipHtml(db, salary) {
  const school = db.schools.find(s => s.id === salary.school_id);
  const teacher = db.teachers.find(t => t.id === salary.teacher_id);
  const state = deriveSalaryState(db, salary);
  const schoolName = school?.name || 'School';
  return `<!doctype html><html><head><meta charset="utf-8"><title>Salary Slip ${salary.id}</title>${renderPrintStyles(school?.primary_color)}</head><body>
    <div class="sheet">
      <div class="watermark">${escapeHtml(schoolName)}</div>
      <div class="head">
        <div class="brand">
          ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : ''}
          <div class="org">
            <h2>${escapeHtml(schoolName)}</h2>
            <small>${escapeHtml(school?.tagline || '')}</small>
            <small>${escapeHtml(school?.address || '')}</small>
          </div>
        </div>
        <div class="doc-title">
          <h3>Teacher Salary Slip</h3>
          <div class="mini">Salary ID: ${salary.id}<br>Month: ${escapeHtml(salary.month_label || '')}</div>
        </div>
      </div>
      <div class="meta">
        <div class="box"><strong>Teacher</strong><div>${escapeHtml(teacher?.name || '')}</div><div>${escapeHtml(teacher?.subject || '')}</div></div>
        <div class="box"><strong>Assigned Class</strong><div>${escapeHtml(teacher?.assigned_class || '')}</div><div>${escapeHtml(teacher?.phone || '')}</div></div>
        <div class="box"><strong>Net Salary</strong><div>Rs. ${Number(state.net_amount || 0).toLocaleString()}</div><div>Status: ${escapeHtml(state.status)}</div></div>
        <div class="box"><strong>Paid / Balance</strong><div>Paid: Rs. ${Number(state.amount_paid || 0).toLocaleString()}</div><div>Balance: Rs. ${Number(state.balance_due || 0).toLocaleString()}</div></div>
      </div>
      <div class="summary">
        <div class="item"><b>Gross</b><span>Rs. ${Number(state.gross_amount || 0).toLocaleString()}</span></div>
        <div class="item"><b>Bonus</b><span>Rs. ${Number(state.bonus_amount || 0).toLocaleString()}</span></div>
        <div class="item"><b>Deductions</b><span>Rs. ${Number(state.deduction_amount || 0).toLocaleString()}</span></div>
      </div>
      <div class="summary" style="margin-top:12px;grid-template-columns:repeat(3,minmax(0,1fr));">
        <div class="item"><b>Net Payable</b><span>Rs. ${Number(state.net_amount || 0).toLocaleString()}</span></div>
        <div class="item"><b>Paid</b><span>Rs. ${Number(state.amount_paid || 0).toLocaleString()}</span></div>
        <div class="item"><b>Balance</b><span>Rs. ${Number(state.balance_due || 0).toLocaleString()}</span></div>
      </div>
      <div class="table-shell">
        <table>
          <tr><th>Description</th><th>Value</th></tr>
          <tr><td>Title</td><td>${escapeHtml(salary.title || 'Monthly Salary')}</td></tr>
          <tr><td>Due Date</td><td>${escapeHtml(salary.due_date || '')}</td></tr>
          <tr><td>Notes</td><td>${escapeHtml(salary.note || salary.notes || '')}</td></tr>
        </table>
      </div>
      <div class="foot">This is a system generated teacher salary slip for school records.</div>
      <div class="actions"><button onclick="window.print()">Print Salary Slip</button></div>
    </div>
  </body></html>`;
}

function renderPayrollSheetHtml(db, schoolId, monthLabel) {
  const school = db.schools.find(s => s.id === schoolId);
  const summary = getPayrollSummary(db, schoolId);
  const month = safeText(monthLabel);
  const rows = db.salaries.filter(s => s.school_id === schoolId && (!month || safeText(s.month_label) === month)).map(s => salaryWithRelations(db, s));
  const totals = rows.reduce((acc, row) => {
    acc.gross += Number(row.gross_amount || 0);
    acc.bonus += Number(row.bonus_amount || 0);
    acc.deduction += Number(row.deduction_amount || 0);
    acc.net += Number(row.net_amount || 0);
    acc.paid += Number(row.amount_paid || 0);
    acc.balance += Number(row.balance_due || 0);
    return acc;
  }, { gross: 0, bonus: 0, deduction: 0, net: 0, paid: 0, balance: 0 });
  return `<!doctype html><html><head><meta charset="utf-8"><title>Payroll Sheet ${escapeHtml(month || 'All Months')}</title>${renderPrintStyles(school?.primary_color)}</head><body>
    <div class="sheet">
      <div class="watermark">${escapeHtml(school?.name || 'School')}</div>
      <div class="head">
        <div class="brand">
          ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : ''}
          <div class="org">
            <h2>${escapeHtml(school?.name || 'School')}</h2>
            <small>${escapeHtml(school?.tagline || '')}</small>
            <small>${escapeHtml(school?.address || '')}</small>
          </div>
        </div>
        <div class="doc-title">
          <h3>Payroll Summary Sheet</h3>
          <div class="mini">Month: ${escapeHtml(month || 'All')}<br>Generated: ${escapeHtml(String(nowIso()).slice(0,10))}</div>
        </div>
      </div>
      <div class="summary">
        <div class="item"><b>Gross</b><span>Rs. ${totals.gross.toLocaleString()}</span></div>
        <div class="item"><b>Bonus</b><span>Rs. ${totals.bonus.toLocaleString()}</span></div>
        <div class="item"><b>Deductions</b><span>Rs. ${totals.deduction.toLocaleString()}</span></div>
      </div>
      <div class="summary" style="margin-top:12px;grid-template-columns:repeat(3,minmax(0,1fr));">
        <div class="item"><b>Net Payroll</b><span>Rs. ${totals.net.toLocaleString()}</span></div>
        <div class="item"><b>Paid</b><span>Rs. ${totals.paid.toLocaleString()}</span></div>
        <div class="item"><b>Balance</b><span>Rs. ${totals.balance.toLocaleString()}</span></div>
      </div>
      <div class="table-shell">
        <table>
          <tr><th>Teacher</th><th>Subject</th><th>Month</th><th>Gross</th><th>Bonus</th><th>Deductions</th><th>Net</th><th>Paid</th><th>Balance</th><th>Status</th></tr>
          ${rows.map(r => `<tr><td>${escapeHtml(r.teacher_name || '')}</td><td>${escapeHtml(r.subject || '')}</td><td>${escapeHtml(r.month_label || '')}</td><td>Rs. ${Number(r.gross_amount || 0).toLocaleString()}</td><td>Rs. ${Number(r.bonus_amount || 0).toLocaleString()}</td><td>Rs. ${Number(r.deduction_amount || 0).toLocaleString()}</td><td>Rs. ${Number(r.net_amount || 0).toLocaleString()}</td><td>Rs. ${Number(r.amount_paid || 0).toLocaleString()}</td><td>Rs. ${Number(r.balance_due || 0).toLocaleString()}</td><td>${escapeHtml(r.status || '')}</td></tr>`).join('') || `<tr><td colspan="10">No payroll records found</td></tr>`}
        </table>
      </div>
      <div class="foot">System generated payroll summary. Monthly summaries available in the admin portal.</div>
      <div class="actions"><button onclick="window.print()">Print Payroll Sheet</button></div>
    </div>
  </body></html>`;
}

function renderResultCardHtml(db, studentId, examTitle) {
  const student = db.students.find(s => s.id === Number(studentId));
  if (!student) return '<h1>Result card not found</h1>';
  const school = db.schools.find(s => s.id === student.school_id);
  const results = db.results.filter(r => r.student_id === student.id && (!examTitle || String(r.exam_title || '') === String(examTitle || '')));
  const attendance = getStudentAttendanceSummary(db, student.id);
  const schoolName = school?.name || 'School Name Goes Here';
  const meta = results[0] || {};
  const grouped = new Map();
  const standardSubjects = ['Reading','Language','Spelling','Writing','Math','Science','Social Studies','Physical Education','Art','Music','Extracurricular'];
  results.forEach(item => {
    const subject = safeText(item.subject) || 'Subject';
    if (!grouped.has(subject)) grouped.set(subject, { term1:'', term2:'', term3:'', term4:'', obtained:'', total: Number(item.total_marks || 0), grade:'', remarks:'' });
    const row = grouped.get(subject);
    const key = inferTermKey(item.exam_title || examTitle);
    row[key] = Number(item.obtained_marks || 0) ? Number(item.obtained_marks || 0) : '';
    row.obtained = Number(item.obtained_marks || 0) || row.obtained;
    row.total = Math.max(Number(row.total || 0), Number(item.total_marks || 0));
    row.grade = item.grade || computeGrade(item.obtained_marks, item.total_marks);
    row.remarks = item.remarks || row.remarks;
  });
  standardSubjects.forEach(subject => {
    if (!grouped.has(subject)) grouped.set(subject, { term1:'', term2:'', term3:'', term4:'', obtained:'', total:'', grade:'', remarks:'' });
  });
  const total = results.reduce((n, item) => n + Number(item.total_marks || 0), 0);
  const obtained = results.reduce((n, item) => n + Number(item.obtained_marks || 0), 0);
  const pct = total > 0 ? ((obtained / total) * 100) : 0;
  const overallGrade = computeGrade(obtained, total || 100);
  const avg = results.length ? (obtained / Math.max(results.length, 1)).toFixed(1) : '0';
  const overallRemarks = safeText(meta.overall_remarks) || 'Maintain focus, complete home practice regularly, and keep improving with confidence.';
  const resultStatus = safeText(meta.result_status) || (pct >= 40 ? 'Promoted' : 'Needs Improvement');
  const positionLabel = safeText(meta.position_label) || 'Overall Result';
  const teacherName = db.teachers.find(t => t.assigned_class === `${student.class_name}-${student.section}` || safeText(t.assigned_class) === safeText(student.class_name))?.name || '';
  const schoolYear = String(examTitle || '').replace(/exam/i,'').trim() || String(new Date().getFullYear());
  const rows = [...grouped.entries()].map(([subject, row]) => `
    <tr>
      <td>${escapeHtml(subject)}</td>
      <td>${row.term1}</td>
      <td>${row.term2}</td>
      <td>${row.term3}</td>
      <td>${row.term4}</td>
      <td>${row.total}</td>
      <td>${row.obtained}</td>
      <td>${escapeHtml(row.grade)}</td>
    </tr>`).join('');
  const summaryTerms = [
    { key: 'term1', label: '1st', value: averageTerm(grouped, 'term1') },
    { key: 'term2', label: '2nd', value: averageTerm(grouped, 'term2') },
    { key: 'term3', label: '3rd', value: averageTerm(grouped, 'term3') },
    { key: 'term4', label: '4th', value: averageTerm(grouped, 'term4') }
  ];
  const brandGreen = school?.primary_color || '#8cc63f';
  const brandNavy = '#0b275c';
  return `<!doctype html><html><head><meta charset="utf-8"><title>Report Card</title>
  <style>
    *{box-sizing:border-box} body{font-family:Arial,sans-serif;background:#eef3f8;margin:0;padding:18px;color:#0f172a}
    .sheet{width:900px;min-height:1160px;margin:0 auto;background:#fff;position:relative;border:1px solid #cad3df;overflow:hidden;box-shadow:0 24px 60px rgba(15,23,42,.08)}
    .sheet:before{content:"";position:absolute;inset:18px;border:1px solid #2b3f60;pointer-events:none}
    .curve-l1,.curve-l2,.curve-r1,.curve-r2{position:absolute;pointer-events:none;border-radius:999px}
    .curve-l1{left:-105px;top:-80px;width:210px;height:260px;border:18px solid ${brandNavy};border-right:none;border-bottom:none}
    .curve-l2{left:-145px;bottom:-120px;width:260px;height:430px;border:30px solid ${brandNavy};border-right:none;border-top:none}
    .curve-l2:after{content:"";position:absolute;left:38px;bottom:-18px;width:220px;height:380px;border:18px solid ${brandGreen};border-right:none;border-top:none;border-radius:inherit}
    .curve-r1{right:-120px;top:-110px;width:270px;height:330px;border:30px solid ${brandGreen};border-left:none;border-bottom:none}
    .curve-r2{right:-70px;top:-42px;width:185px;height:240px;border:18px solid ${brandNavy};border-left:none;border-bottom:none}
    .orn-top,.orn-bottom{position:absolute;font-size:34px;color:${brandNavy};opacity:.9;font-weight:700;line-height:1}.orn-top{left:24px;top:24px}.orn-bottom{right:28px;bottom:24px;transform:rotate(180deg)}
    .watermark-word{position:absolute;inset:0;display:grid;place-items:center;font-size:88px;font-weight:800;letter-spacing:.08em;color:${brandNavy};opacity:.04;transform:rotate(-28deg);pointer-events:none}
    .wm-logo{position:absolute;inset:0;display:grid;place-items:center;opacity:.06;pointer-events:none}.wm-logo img{max-width:340px;max-height:340px;object-fit:contain;filter:grayscale(100%)}
    .inner{position:relative;z-index:1;padding:54px 56px 28px 56px}
    .head{display:flex;align-items:flex-start;gap:14px;padding-left:34px}
    .logo{width:58px;height:58px;object-fit:contain;border-radius:14px}
    .fallback-logo{width:58px;height:58px;border-radius:14px;background:${brandGreen};display:grid;place-items:center;color:#fff;font-weight:900;font-size:24px}
    .title h1{margin:0;font-size:28px;letter-spacing:.02em;color:#111;font-weight:800}
    .title .school{font-size:13px;color:#374151;margin-top:3px}
    .meta{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:28px;padding:0 34px}
    .line{display:grid;grid-template-columns:115px 1fr;gap:8px;align-items:center;margin-bottom:12px}.line label{font-size:12px;font-weight:700;color:#334155}.fill{min-height:24px;border-bottom:1px solid #55657d;padding:2px 6px 3px;font-size:13px}
    .main-table{margin-top:10px;padding:0 24px 0 24px} table{width:100%;border-collapse:collapse} .marks-table th,.marks-table td{border:1px solid #334155;padding:6px 7px;font-size:11px;text-align:center}.marks-table th:first-child,.marks-table td:first-child{text-align:left}.marks-table thead th{background:${brandNavy};color:#fff;font-weight:700}
    .two-col{display:grid;grid-template-columns:1.55fr .9fr;gap:18px;margin-top:12px;padding:0 24px}
    .feedback-title{font-size:11px;font-weight:800;text-align:right;color:#1f2937;margin-bottom:8px}
    .feedback-box{min-height:92px;border-bottom:1px solid #334155;position:relative;padding-top:10px;font-size:12px;line-height:1.8;background:linear-gradient(to bottom, transparent 28px, rgba(51,65,85,.12) 29px, transparent 30px) 0 0/100% 28px}
    .summary-table td,.summary-table th{border:1px solid #334155;padding:6px 7px;font-size:11px;text-align:center}.summary-table .dark{background:${brandNavy};color:#fff;font-weight:700}.summary-table .light{background:#f4f7fb;font-weight:700}
    .footer{display:grid;grid-template-columns:1fr auto 1fr;gap:20px;align-items:end;margin-top:24px;padding:0 24px 12px}.days{font-size:12px;color:#111}.days b{font-weight:800}.sign{text-align:center;font-size:12px;color:#111}.sign .line-sign{border-top:1px solid #334155;margin-top:28px;padding-top:6px}
    .result-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:18px 24px 14px}.pill{border:1px solid #d4dceb;border-radius:14px;padding:10px 12px;background:#f9fbff}.pill span{display:block;font-size:10px;font-weight:800;text-transform:uppercase;color:#475569;letter-spacing:.04em;margin-bottom:4px}.pill strong{font-size:16px;color:#0f172a}
    .actions{padding:0 24px 22px}.actions button{border:none;background:${brandNavy};color:#fff;border-radius:10px;padding:10px 14px;font-weight:700;cursor:pointer}
    @media print{body{background:#fff;padding:0}.sheet{box-shadow:none;border:none;width:auto;min-height:auto}.actions{display:none}}
  </style></head><body>
    <div class="sheet">
      <div class="curve-l1"></div><div class="curve-l2"></div><div class="curve-r1"></div><div class="curve-r2"></div>
      <div class="orn-top">❦</div><div class="orn-bottom">❦</div>
      ${school?.logo_url ? `<div class="wm-logo"><img src="${escapeHtml(school.logo_url)}" alt="watermark"></div>` : ''}
      <div class="watermark-word">${escapeHtml(schoolName)}</div>
      <div class="inner">
        <div class="head">
          ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : `<div class="fallback-logo">${escapeHtml((schoolName || 'S').slice(0,1))}</div>`}
          <div class="title"><h1>REPORT CARD</h1><div class="school">${escapeHtml(schoolName)}</div></div>
        </div>
        <div class="meta">
          <div>
            <div class="line"><label>Student Name:</label><div class="fill">${escapeHtml(student.name || '')}</div></div>
            <div class="line"><label>School Year:</label><div class="fill">${escapeHtml(schoolYear)}</div></div>
            <div class="line"><label>Roll No:</label><div class="fill">${escapeHtml(student.roll_no || '')}</div></div>
          </div>
          <div>
            <div class="line"><label>Class/Section:</label><div class="fill">${escapeHtml(student.class_name || '')} ${escapeHtml(student.section || '')}</div></div>
            <div class="line"><label>Teacher Name:</label><div class="fill">${escapeHtml(teacherName)}</div></div>
            <div class="line"><label>Parent Name:</label><div class="fill">${escapeHtml(student.father_name || '')}</div></div>
          </div>
        </div>
        <div class="result-strip">
          <div class="pill"><span>Overall Result</span><strong>${escapeHtml(resultStatus)}</strong></div>
          <div class="pill"><span>Position / Merit</span><strong>${escapeHtml(positionLabel)}</strong></div>
          <div class="pill"><span>Percentage</span><strong>${pct.toFixed(1)}%</strong></div>
          <div class="pill"><span>Overall Grade</span><strong>${escapeHtml(overallGrade)}</strong></div>
        </div>
        <div class="main-table">
          <table class="marks-table">
            <thead>
              <tr><th>Subject</th><th>1<sup>st</sup> Term</th><th>2<sup>nd</sup> Term</th><th>3<sup>rd</sup> Term</th><th>4<sup>th</sup> Term</th><th>Total</th><th>Obtained</th><th>Grade</th></tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
        <div class="two-col">
          <div>
            <div class="feedback-title">TEACHER'S FEEDBACK</div>
            <div class="feedback-box">${escapeHtml(overallRemarks)}</div>
          </div>
          <div>
            <table class="summary-table">
              <tr><th class="dark" colspan="5">Terms Based Grades</th></tr>
              <tr><th class="light"></th>${summaryTerms.map(t => `<th class="light">${t.label}</th>`).join('')}</tr>
              <tr><td class="light">Quarterly Grade</td>${summaryTerms.map(t => `<td>${escapeHtml(computeGrade(t.value, 100))}</td>`).join('')}</tr>
              <tr><td class="light">Average Grade</td>${summaryTerms.map(t => `<td>${t.value ? `${Number(t.value).toFixed(1)}%` : ''}</td>`).join('')}</tr>
              <tr><td class="light">Total Obtained</td><td colspan="4">${Number(obtained || 0).toLocaleString()}</td></tr>
              <tr><td class="light">Total Marks</td><td colspan="4">${Number(total || 0).toLocaleString()}</td></tr>
              <tr><td class="light">Average Score</td><td colspan="4">${escapeHtml(avg)}</td></tr>
            </table>
          </div>
        </div>
        <div class="footer">
          <div class="days">Total School Days: <b>${attendance.total}</b>&nbsp;&nbsp;&nbsp; Attended: <b>${attendance.present}</b>&nbsp;&nbsp;&nbsp; Absent: <b>${attendance.absent}</b></div>
          <div class="sign"><div class="line-sign">Class Teacher</div></div>
          <div class="sign"><div class="line-sign">Principal / Admin</div></div>
        </div>
        <div class="actions"><button onclick="window.print()">Print Result Card</button></div>
      </div>
    </div>
  </body></html>`;
}

function averageTerm(grouped, key) {
  const values = [...grouped.values()].map(row => Number(row[key] || 0)).filter(v => v > 0);
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}


function renderAdmissionSlipHtml(db, admission) {
  const school = db.schools.find(s => s.id === admission.school_id);
  const schoolName = school?.name || 'School';
  return `<!doctype html><html><head><meta charset="utf-8"><title>Admission Slip</title>${renderPrintStyles(school?.primary_color)}</head><body>
    <div class="sheet">
      <div class="watermark">${escapeHtml(schoolName)}</div>
      <div class="head">
        <div class="brand">
          ${school?.logo_url ? `<img class="logo" src="${escapeHtml(school.logo_url)}" alt="logo">` : ''}
          <div class="org">
            <h2>${escapeHtml(schoolName)}</h2>
            <small>${escapeHtml(school?.tagline || '')}</small>
            <small>${escapeHtml(school?.address || '')}</small>
          </div>
        </div>
        <div class="doc-title">
          <h3>Admission Confirmation Slip</h3>
          <div class="mini">Application ID: ${admission.id}<br>Approved: ${escapeHtml(String(admission.approved_at || '').slice(0, 10))}</div>
        </div>
      </div>
      <div class="meta">
        <div class="box"><strong>Student</strong><div>${escapeHtml(admission.student_name || '')}</div><div>Roll No: ${escapeHtml(admission.generated_roll_no || '')}</div></div>
        <div class="box"><strong>Class / Section</strong><div>${escapeHtml(admission.class_name || '')}</div><div>Section: ${escapeHtml(admission.generated_section || '')}</div></div>
        <div class="box"><strong>Parent / Guardian</strong><div>${escapeHtml(admission.father_name || '')}</div><div>${escapeHtml(admission.phone || '')}</div></div>
        <div class="box"><strong>Admission Fee</strong><div>Rs. ${Number(admission.admission_fee || 0).toLocaleString()}</div><div>Status: ${escapeHtml(admission.status || '')}</div></div>
      </div>
      <div class="copies">
        <div class="copy">
          <h4>Student Portal Login</h4>
          <p><strong>Login ID:</strong> ${escapeHtml(admission.student_portal_email || '')}</p>
          <p><strong>Password:</strong> ${escapeHtml(admission.student_portal_password || '')}</p>
        </div>
        <div class="copy">
          <h4>Parent Portal Login</h4>
          <p><strong>Login ID:</strong> ${escapeHtml(admission.parent_portal_email || '')}</p>
          <p><strong>Password:</strong> ${escapeHtml(admission.parent_portal_password || '')}</p>
        </div>
        <div class="copy">
          <h4>Instructions</h4>
          <p>Please keep this slip safe for future login, result access, fee challan download and profile updates.</p>
        </div>
      </div>
      <div class="foot">This admission slip is system generated and can be printed for the parent record and office record.</div>
      <div class="actions"><button onclick="window.print()">Print Admission Slip</button></div>
    </div>
  </body></html>`;
}

function createDemoDb() {
  const db = createEmptyDb();
  db.meta.initialized = true;
  const school = {
    id: nextId(db, 'schools'),
    name: 'Bright Future School',
    code: 'BFS001',
    address: 'Sahiwal, Pakistan',
    phone: '0300-1111111',
    tagline: 'Learning with discipline and care',
    logo_url: '',
    primary_color: '#2146d0',
    whatsapp_number: '923001111111',
    created_at: nowIso()
  };
  db.schools.push(school);

  const owner = {
    id: nextId(db, 'users'),
    school_id: null,
    role: 'owner',
    name: 'Portal Owner',
    email: 'owner@portal.local',
    password_hash: hashPassword('Owner@123'),
    linked_parent_id: null,
    linked_student_id: null,
    is_active: true,
    created_at: nowIso()
  };
  const admin = {
    id: nextId(db, 'users'),
    school_id: school.id,
    role: 'admin',
    name: 'BFS Admin',
    email: 'admin@bfs.local',
    password_hash: hashPassword('Admin@123'),
    linked_parent_id: null,
    linked_student_id: null,
    is_active: true,
    created_at: nowIso()
  };
  db.users.push(owner, admin);

  const parent = {
    id: nextId(db, 'parents'),
    school_id: school.id,
    name: 'Muhammad Aslam',
    phone: '923005551111',
    email: 'aslam.parent@bfs.local',
    address: 'Street 1, Sahiwal',
    created_at: nowIso()
  };
  db.parents.push(parent);

  const student = {
    id: nextId(db, 'students'),
    school_id: school.id,
    parent_id: parent.id,
    roll_no: '101',
    name: 'Ahmad Ali',
    class_name: '5',
    section: 'A',
    father_name: 'Muhammad Aslam',
    phone: '923007771111',
    address: 'Street 1, Sahiwal',
    created_at: nowIso()
  };
  db.students.push(student);

  db.users.push({
    id: nextId(db, 'users'),
    school_id: school.id,
    role: 'parent',
    name: parent.name,
    email: 'parent1@bfs.local',
    password_hash: hashPassword('Parent@123'),
    linked_parent_id: parent.id,
    linked_student_id: null,
    is_active: true,
    created_at: nowIso()
  });
  db.users.push({
    id: nextId(db, 'users'),
    school_id: school.id,
    role: 'student',
    name: student.name,
    email: 'student1@bfs.local',
    password_hash: hashPassword('Student@123'),
    linked_parent_id: null,
    linked_student_id: student.id,
    is_active: true,
    created_at: nowIso()
  });

  const teacher = {
    id: nextId(db, 'teachers'),
    school_id: school.id,
    name: 'Sir Hamza',
    subject: 'Mathematics',
    assigned_class: '5-A',
    phone: '923006661111',
    email: 'teacher1@bfs.local',
    created_at: nowIso()
  };
  db.teachers.push(teacher);
  db.users.push({
    id: nextId(db, 'users'),
    school_id: school.id,
    role: 'teacher',
    name: teacher.name,
    email: teacher.email,
    password_hash: hashPassword('Teacher@123'),
    linked_parent_id: null,
    linked_student_id: null,
    linked_teacher_id: teacher.id,
    is_active: true,
    created_at: nowIso()
  });

  db.notices.push({
    id: nextId(db, 'notices'),
    school_id: school.id,
    title: 'April Monthly Test Schedule',
    body: 'Monthly tests will start from 15 April. Students should bring their admit slips and stationery.',
    audience: 'all',
    created_by: admin.id,
    created_at: nowIso()
  });

  db.results.push({
    id: nextId(db, 'results'),
    school_id: school.id,
    student_id: student.id,
    exam_title: 'Mid Term 2026',
    subject: 'Mathematics',
    total_marks: 100,
    obtained_marks: 86,
    grade: 'A',
    remarks: 'Very good performance',
    created_at: nowIso()
  });
  db.results.push({
    id: nextId(db, 'results'),
    school_id: school.id,
    student_id: student.id,
    exam_title: 'Mid Term 2026',
    subject: 'English',
    total_marks: 100,
    obtained_marks: 81,
    grade: 'A',
    remarks: 'Good expression',
    created_at: nowIso()
  });

  const fee = {
    id: nextId(db, 'fees'),
    school_id: school.id,
    student_id: student.id,
    title: 'Monthly Tuition Fee',
    month_label: 'April 2026',
    amount: 4500,
    due_date: '2026-04-20',
    notes: 'Regular monthly fee',
    status: 'unpaid',
    original_amount: 4500,
    balance_due: 4500,
    amount_paid: 0,
    created_by: admin.id,
    created_at: nowIso()
  };
  db.fees.push(fee);

  const salary = {
    id: nextId(db, 'salaries'),
    school_id: school.id,
    teacher_id: teacher.id,
    title: 'Monthly Salary',
    month_label: 'April 2026',
    gross_amount: 30000,
    bonus_amount: 2000,
    deduction_amount: 500,
    net_amount: 31500,
    amount_paid: 10000,
    balance_due: 21500,
    due_date: '2026-04-30',
    note: 'First installment paid',
    status: 'partial',
    created_by: admin.id,
    created_at: nowIso()
  };
  db.salaries.push(salary);
  db.salaryPayments.push({
    id: nextId(db, 'salaryPayments'),
    school_id: school.id,
    salary_id: salary.id,
    amount: 10000,
    previous_balance: 31500,
    remaining_balance: 21500,
    payment_type: 'partial',
    method: 'Cash',
    note: 'Advance salary installment',
    status: 'paid',
    created_at: nowIso()
  });
  return db;
}

function normalizeCsvHeader(value) {
  return String(value || '')
    .replace(/^﻿/, '')
    .trim()
    .toLowerCase()
    .replace(/[()]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function extractGoogleSheetCsvUrl(input) {
  const raw = String(input || '').trim();
  if (!raw) return raw;
  try {
    const parsed = new URL(raw);
    if (!/docs\.google\.com$/i.test(parsed.hostname)) return raw;
    if (!parsed.pathname.includes('/spreadsheets/')) return raw;

    const match = parsed.pathname.match(/\/d\/([^/]+)/);
    if (!match) return raw;

    const spreadsheetId = match[1];
    const gid = parsed.searchParams.get('gid') || '0';
    return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=csv&gid=${gid}`;
  } catch {
    return raw;
  }
}

function parseCsvText(text) {
  const rows = [];
  let row = [];
  let value = '';
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];

    if (ch === '"') {
      if (quoted && text[i + 1] === '"') {
        value += '"';
        i += 1;
      } else {
        quoted = !quoted;
      }
      continue;
    }

    if (ch === ',' && !quoted) {
      row.push(value.trim());
      value = '';
      continue;
    }

    if ((ch === '\n' || ch === '\r') && !quoted) {
      if (ch === '\r' && text[i + 1] === '\n') i += 1;
      row.push(value.trim());
      const hasContent = row.some(cell => String(cell || '').trim() !== '');
      if (hasContent) rows.push(row);
      row = [];
      value = '';
      continue;
    }

    value += ch;
  }

  if (value.length || row.length) {
    row.push(value.trim());
    const hasContent = row.some(cell => String(cell || '').trim() !== '');
    if (hasContent) rows.push(row);
  }

  return rows;
}

async function parsePublicCsv(url) {
  const fetchUrl = extractGoogleSheetCsvUrl(url);
  const response = await fetch(fetchUrl, {
    headers: {
      'User-Agent': 'SchoolPortal/1.0',
      'Accept': 'text/csv,text/plain,application/octet-stream,*/*'
    }
  });
  if (!response.ok) throw new Error('Could not fetch Google Sheet CSV link.');
  const text = await response.text();
  if (!text.trim()) return [];
  if (/<!doctype html|<html[\s>]/i.test(text)) {
    throw new Error('Please use a public Google Sheet link or publish the sheet as CSV.');
  }

  const rows = parseCsvText(text);
  if (!rows.length) return [];
  const headers = rows.shift().map(normalizeCsvHeader);
  return rows.map(cols => Object.fromEntries(headers.map((h, i) => [h, cols[i] || ''])));
}

function createSchoolAndAdmin(db, body) {
  const code = normalizeCode(body.code);
  if (!safeText(body.name)) throw new Error('School name is required.');
  if (!code) throw new Error('School code is required.');
  if (schoolByCode(db, code)) throw new Error('This school code already exists.');
  const adminEmail = normalizeEmail(body.adminEmail);
  if (!adminEmail) throw new Error('Admin email is required.');
  if (db.users.some(u => u.email === adminEmail)) throw new Error('This admin email is already in use.');

  const school = {
    id: nextId(db, 'schools'),
    name: safeText(body.name),
    code,
    address: safeText(body.address),
    phone: safeText(body.phone),
    tagline: safeText(body.tagline),
    logo_url: chooseLogoValue(body),
    primary_color: safeText(body.primaryColor) || '#2146d0',
    whatsapp_number: safeText(body.whatsappNumber),
    created_at: nowIso()
  };
  db.schools.push(school);

  db.users.push({
    id: nextId(db, 'users'),
    school_id: school.id,
    role: 'admin',
    name: safeText(body.adminName) || 'School Admin',
    email: adminEmail,
    password_hash: hashPassword(body.adminPassword || 'Admin@123'),
    linked_parent_id: null,
    linked_student_id: null,
    is_active: true,
    created_at: nowIso()
  });

  return school;
}

function createOwnerUser(db, body) {
  const email = normalizeEmail(body.ownerEmail);
  if (!email) throw new Error('Owner email is required.');
  if (db.users.some(u => u.email === email)) throw new Error('This owner email already exists.');
  db.users.push({
    id: nextId(db, 'users'),
    school_id: null,
    role: 'owner',
    name: safeText(body.ownerName) || 'Portal Owner',
    email,
    password_hash: hashPassword(body.ownerPassword || 'Owner@123'),
    linked_parent_id: null,
    linked_student_id: null,
    is_active: true,
    created_at: nowIso()
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const fullUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = fullUrl.pathname;

    if (req.method === 'GET' && pathname === '/api/health') {
      return json(res, 200, { ok: true, time: nowIso() });
    }

    if (req.method === 'GET' && pathname === '/api/public/config') {
      const db = readDb();
      return json(res, 200, { initialized: !!db.meta.initialized, schools: db.schools.length });
    }

    if (req.method === 'GET' && pathname === '/api/public/schools') {
      const db = readDb();
      return json(res, 200, db.schools.map(s => ({ id: s.id, name: s.name, code: s.code })));
    }

    if (req.method === 'POST' && pathname === '/api/setup/initialize') {
      const existing = readDb();
      if (existing.meta.initialized) return json(res, 409, { message: 'Portal is already initialized.' });
      const body = await parseBody(req);
      const db = createEmptyDb();
      createOwnerUser(db, body);
      createSchoolAndAdmin(db, body);
      db.meta.initialized = true;
      writeDb(db);
      return json(res, 201, { message: 'Portal setup completed. Please log in.' });
    }

    if (req.method === 'POST' && pathname === '/api/setup/load-demo') {
      const existing = readDb();
      if (existing.meta.initialized) return json(res, 409, { message: 'Portal is already initialized.' });
      const db = createDemoDb();
      writeDb(db);
      return json(res, 201, { message: 'Demo data loaded successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/public/onboarding-request') {
      const body = await parseBody(req);
      const db = readDb();
      db.onboardingRequests.push({
        id: nextId(db, 'onboardingRequests'),
        school_name: safeText(body.schoolName),
        city: safeText(body.city),
        contact_name: safeText(body.contactName),
        phone: safeText(body.phone),
        email: normalizeEmail(body.email),
        notes: safeText(body.notes),
        status: 'new',
        created_at: nowIso()
      });
      writeDb(db);
      return json(res, 201, { message: 'School onboarding request submitted.' });
    }

    if (req.method === 'POST' && pathname === '/api/auth/request-reset') {
      const body = await parseBody(req);
      const db = readDb();
      const email = normalizeEmail(body.email);
      const schoolCode = normalizeCode(body.schoolCode);
      const roleHint = safeText(body.roleHint).toLowerCase();
      if (roleHint === 'owner') return json(res, 400, { message: 'Owner password can be changed from the owner dashboard.' });
      const school = schoolCode ? schoolByCode(db, schoolCode) : null;
      const user = db.users.find(u => u.email === email && (u.role === 'owner' || (school && u.school_id === school.id)));
      const handledByRole = roleHint === 'admin' ? 'owner' : 'admin';
      db.resetRequests.push({
        id: nextId(db, 'resetRequests'),
        email,
        school_id: school?.id || user?.school_id || null,
        school_code: schoolCode,
        role_hint: roleHint,
        handled_by_role: handledByRole,
        note: safeText(body.note),
        status: user ? 'pending' : 'not-found',
        created_at: nowIso()
      });
      writeDb(db);
      return json(res, 201, { message: handledByRole === 'owner' ? 'Admin reset request owner dashboard mein chali gayi hai.' : 'Student/Parent/Teacher reset request admin dashboard mein chali gayi hai.' });
    }

    if (req.method === 'POST' && pathname === '/api/auth/login') {
      const body = await parseBody(req);
      const db = readDb();
      if (!db.meta.initialized) return json(res, 400, { message: 'Portal setup is not completed yet.' });
      const email = normalizeEmail(body.email);
      const password = String(body.password || '');
      const schoolCode = normalizeCode(body.schoolCode);
      const user = db.users.find(u => u.email === email && u.is_active !== false);
      if (!user) return json(res, 401, { message: 'Invalid email or password.' });
      if (user.role !== 'owner') {
        if (!schoolCode) return json(res, 400, { message: 'School code is required for this login.' });
        const school = schoolByCode(db, schoolCode);
        if (!school || school.id !== user.school_id) return json(res, 401, { message: 'Wrong school code for this account.' });
      }
      if (!verifyPassword(password, user.password_hash)) return json(res, 401, { message: 'Invalid email or password.' });
      const token = createToken();
      sessions.set(token, { user_id: user.id, created_at: nowIso() });
      return json(res, 200, { token, user: sanitizeUser(db, user) });
    }

    if (req.method === 'GET' && pathname === '/api/me') {
      const auth = requireAuth(req, res, null);
      if (!auth) return;
      return json(res, 200, { user: sanitizeUser(auth.db, auth.user) });
    }

    if (req.method === 'GET' && pathname === '/api/owner/stats') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      const { db } = auth;
      return json(res, 200, {
        schools: db.schools.length,
        students: db.students.length,
        parents: db.parents.length,
        teachers: db.teachers.length,
        unpaid: db.fees.reduce((n, fee) => n + Number(deriveFeeState(db, fee).balance_due || 0), 0),
        onboardingRequests: db.onboardingRequests.length,
        resetRequests: db.resetRequests.filter(r => r.status === 'pending' && r.handled_by_role === 'owner').length
      });
    }

    if (req.method === 'GET' && pathname === '/api/owner/schools') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      const schools = auth.db.schools.map(s => ({
        ...s,
        students_count: auth.db.students.filter(x => x.school_id === s.id).length,
        parents_count: auth.db.parents.filter(x => x.school_id === s.id).length,
        teachers_count: auth.db.teachers.filter(x => x.school_id === s.id).length,
        admins_count: auth.db.users.filter(x => x.school_id === s.id && x.role === 'admin').length
      }));
      return json(res, 200, schools);
    }

    if (req.method === 'POST' && pathname === '/api/owner/schools') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      const body = await parseBody(req);
      const school = createSchoolAndAdmin(auth.db, body);
      writeDb(auth.db);
      return json(res, 201, { message: `School ${school.name} created successfully.` });
    }

    if (req.method === 'PATCH' && pathname === '/api/owner/schools/branding') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      const body = await parseBody(req);
      const school = auth.db.schools.find(s => s.id === Number(body.schoolId));
      if (!school) return json(res, 404, { message: 'School not found.' });
      school.name = safeText(body.name) || school.name;
      school.tagline = safeText(body.tagline);
      school.address = safeText(body.address);
      school.phone = safeText(body.phone);
      school.logo_url = chooseLogoValue(body, school.logo_url);
      school.primary_color = safeText(body.primaryColor) || school.primary_color || '#2146d0';
      school.whatsapp_number = safeText(body.whatsappNumber);
      writeDb(auth.db);
      return json(res, 200, { message: 'School branding updated successfully.' });
    }

    if (req.method === 'GET' && pathname === '/api/owner/requests') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      return json(res, 200, {
        onboarding: auth.db.onboardingRequests.slice().reverse(),
        resets: auth.db.resetRequests.filter(r => r.handled_by_role === 'owner').slice().reverse()
      });
    }

    if (req.method === 'POST' && pathname === '/api/owner/reset-password') {
      const auth = requireAuth(req, res, ['owner']);
      if (!auth) return;
      const body = await parseBody(req);
      const user = auth.db.users.find(u => normalizeEmail(u.email) === normalizeEmail(body.email));
      if (!user) return json(res, 404, { message: 'User not found.' });
      user.password_hash = hashPassword(body.newPassword || 'ChangeMe@123');
      const request = auth.db.resetRequests.find(r => r.id === Number(body.requestId) && r.handled_by_role === 'owner');
      if (request) {
        request.status = 'completed';
        request.completed_at = nowIso();
        request.completed_by = auth.user.id;
      }
      writeDb(auth.db);
      return json(res, 200, { message: 'Password updated successfully.' });
    }

    if (req.method === 'GET' && pathname === '/api/admin/overview') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const scoped = getSchoolScoped(auth.db, auth.user.school_id);
      return json(res, 200, {
        school: scoped.school,
        students: scoped.students.length,
        parents: scoped.parents.length,
        teachers: scoped.teachers.length,
        unpaid: scoped.fees.reduce((n, fee) => n + Number(deriveFeeState(auth.db, fee).balance_due || 0), 0),
        submitted: scoped.payments.filter(p => p.status === 'submitted').length,
        paid: scoped.payments.filter(p => p.status === 'paid').length,
        attendance: scoped.attendance.length,
        results: scoped.results.length,
        notifications: scoped.notifications.length,
        salary_pending: scoped.salaries.reduce((n, salary) => n + Number(deriveSalaryState(auth.db, salary).balance_due || 0), 0)
      });
    }

    if (req.method === 'GET' && pathname === '/api/admin/students') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const students = getSchoolScoped(auth.db, auth.user.school_id).students.map(s => {
        const parent = auth.db.parents.find(p => p.id === s.parent_id);
        const user = findPortalUserByLinked(auth.db, 'student', 'linked_student_id', s.id, auth.user.school_id);
        const totals = getStudentFeeTotals(auth.db, s.id);
        const attendance = getStudentAttendanceSummary(auth.db, s.id);
        return {
          ...s,
          parent_name: parent?.name || '',
          parent_phone: parent?.phone || '',
          parent_email: parent?.email || '',
          portal_email: user?.email || '',
          pending_fee: totals.pending,
          total_paid: totals.paid,
          attendance_present: attendance.present,
          attendance_absent: attendance.absent,
          attendance_total: attendance.total
        };
      });
      return json(res, 200, students);
    }

    if (req.method === 'GET' && pathname === '/api/admin/parents') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).parents.map(parent => {
        const summary = getParentSummary(auth.db, parent.id, auth.user.school_id);
        const portal = findPortalUserByLinked(auth.db, 'parent', 'linked_parent_id', parent.id, auth.user.school_id);
        return { ...parent, ...summary, portal_email: portal?.email || '' };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/admin/teachers') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).teachers.map(teacher => {
        const portal = findPortalUserByLinked(auth.db, 'teacher', 'linked_teacher_id', teacher.id, auth.user.school_id);
        const salaryTotals = getTeacherSalaryTotals(auth.db, teacher.id);
        return { ...teacher, portal_email: portal?.email || '', salary_total: salaryTotals.total, salary_paid: salaryTotals.paid, salary_pending: salaryTotals.pending };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/admin/salaries') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).salaries.map(salary => ({
        ...salaryWithRelations(auth.db, salary),
        slip_url: buildSalarySlipUrl(salary.id),
        payroll_sheet_url: buildPayrollSheetUrl(salary.month_label)
      })));
    }

    if (req.method === 'GET' && pathname === '/api/admin/payroll-summary') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const summary = getPayrollSummary(auth.db, auth.user.school_id);
      return json(res, 200, {
        months: summary.months.map(row => ({ ...row, payroll_sheet_url: buildPayrollSheetUrl(row.month_label) })),
        teachers: summary.teachers
      });
    }

    if (req.method === 'GET' && pathname === '/api/admin/salary-payments') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).salaryPayments.map(payment => {
        const salary = auth.db.salaries.find(s => s.id === payment.salary_id);
        const teacher = salary ? auth.db.teachers.find(t => t.id === salary.teacher_id) : null;
        return {
          ...payment,
          salary_title: salary?.title || 'Salary',
          month_label: salary?.month_label || '',
          teacher_name: teacher?.name || '',
          slip_url: salary ? buildSalarySlipUrl(salary.id) : ''
        };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/admin/fees') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).fees.map(f => {
        const related = feeWithRelations(auth.db, f);
        return {
          ...related,
          ...buildFeeShareLinks(req, f, related.parent_phone, related.student_name)
        };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/admin/payments') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).payments.map(p => ({
        ...paymentWithRelations(auth.db, p),
        receipt_url: buildReceiptUrl(p.id)
      })));
    }

    if (req.method === 'GET' && pathname === '/api/admin/results') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).results.map(r => ({
        ...r,
        student_name: auth.db.students.find(s => s.id === r.student_id)?.name || '',
        result_card_url: buildResultCardUrl(r.student_id, r.exam_title)
      })));
    }

    if (req.method === 'GET' && pathname === '/api/admin/attendance') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).attendance.map(a => {
        const student = auth.db.students.find(s => s.id === a.student_id);
        return { ...a, student_name: student?.name || '', class_name: student?.class_name || '', section: student?.section || '' };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/admin/notifications') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).notifications.slice().reverse());
    }

    if (req.method === 'GET' && pathname === '/api/admin/notices') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).notices.slice().reverse());
    }

    if (req.method === 'POST' && pathname === '/api/admin/notices') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      auth.db.notices.push({
        id: nextId(auth.db, 'notices'),
        school_id: auth.user.school_id,
        title: safeText(body.title),
        body: safeText(body.body),
        audience: safeText(body.audience) || 'all',
        created_by: auth.user.id,
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Notice published successfully.' });
    }

    if (req.method === 'GET' && pathname === '/api/admin/admissions') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const rows = getSchoolScoped(auth.db, auth.user.school_id).admissionRequests.slice().sort((a,b) => (a.status === 'approved') - (b.status === 'approved') || String(b.created_at).localeCompare(String(a.created_at))).map(a => ({ ...a, slip_url: a.status === 'approved' ? `/admission-slip/${a.id}` : '' }));
      return json(res, 200, rows);
    }


    if (req.method === 'GET' && pathname === '/api/admin/reset-requests') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, auth.db.resetRequests.filter(r => r.school_id === auth.user.school_id && r.handled_by_role === 'admin').slice().reverse());
    }

    if (req.method === 'POST' && pathname === '/api/admin/reset-password') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const request = auth.db.resetRequests.find(r => r.id === Number(body.requestId) && r.school_id === auth.user.school_id && r.handled_by_role === 'admin');
      if (!request) return json(res, 404, { message: 'Reset request not found.' });
      const user = auth.db.users.find(u => normalizeEmail(u.email) === normalizeEmail(body.email) && u.school_id === auth.user.school_id);
      if (!user) return json(res, 404, { message: 'User not found.' });
      user.password_hash = hashPassword(body.newPassword || 'ChangeMe@123');
      request.status = 'completed';
      request.completed_at = nowIso();
      request.completed_by = auth.user.id;
      writeDb(auth.db);
      return json(res, 200, { message: 'Password updated successfully by admin.' });
    }

    if (req.method === 'POST' && pathname === '/api/public/admissions') {
      const body = await parseBody(req);
      const db = readDb();
      const school = resolveSchool(db, body.schoolCode, body.schoolName);
      if (!school) return json(res, 404, { message: 'School not found. Please choose a valid school or school code.' });
      db.admissionRequests.push({
        id: nextId(db, 'admissionRequests'),
        school_id: school.id,
        school_code: school.code,
        school_name: school.name,
        student_name: safeText(body.studentName),
        father_name: safeText(body.fatherName),
        class_name: safeText(body.className),
        previous_school: safeText(body.previousSchool),
        dob: safeText(body.dob),
        phone: safeText(body.phone),
        whatsapp: safeText(body.whatsapp),
        email: normalizeEmail(body.email),
        address: safeText(body.address),
        notes: safeText(body.notes),
        status: 'submitted',
        created_at: nowIso()
      });
      writeDb(db);
      return json(res, 201, { message: 'Admission form submitted successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/parents') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const email = normalizeEmail(body.email);
      if (email && auth.db.users.some(u => u.email === email)) return json(res, 409, { message: 'Email already exists.' });
      const parent = {
        id: nextId(auth.db, 'parents'),
        school_id: auth.user.school_id,
        name: safeText(body.name),
        phone: safeText(body.phone),
        email,
        address: safeText(body.address),
        created_at: nowIso()
      };
      auth.db.parents.push(parent);
      if (email && body.portalPassword) {
        auth.db.users.push({
          id: nextId(auth.db, 'users'),
          school_id: auth.user.school_id,
          role: 'parent',
          name: parent.name,
          email,
          password_hash: hashPassword(body.portalPassword),
          linked_parent_id: parent.id,
          linked_student_id: null,
          is_active: true,
          created_at: nowIso()
        });
      }
      writeDb(auth.db);
      return json(res, 201, { message: 'Parent added successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/students') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = {
        id: nextId(auth.db, 'students'),
        school_id: auth.user.school_id,
        parent_id: body.parentId ? Number(body.parentId) : null,
        roll_no: safeText(body.rollNo),
        name: safeText(body.name),
        class_name: safeText(body.className),
        section: safeText(body.section),
        father_name: safeText(body.fatherName),
        phone: safeText(body.phone),
        address: safeText(body.address),
        created_at: nowIso()
      };
      auth.db.students.push(student);
      const email = normalizeEmail(body.studentEmail);
      if (email && body.studentPassword) {
        if (auth.db.users.some(u => u.email === email)) return json(res, 409, { message: 'Student portal email already exists.' });
        auth.db.users.push({
          id: nextId(auth.db, 'users'),
          school_id: auth.user.school_id,
          role: 'student',
          name: student.name,
          email,
          password_hash: hashPassword(body.studentPassword),
          linked_parent_id: null,
          linked_student_id: student.id,
          is_active: true,
          created_at: nowIso()
        });
      }
      writeDb(auth.db);
      return json(res, 201, { message: 'Student added successfully.' });
    }


    const approveAdmissionMatch = pathname.match(/^\/api\/admin\/admissions\/(\d+)\/approve$/);
    if (req.method === 'POST' && approveAdmissionMatch) {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const admission = auth.db.admissionRequests.find(a => a.id === Number(approveAdmissionMatch[1]) && a.school_id === auth.user.school_id);
      if (!admission) return json(res, 404, { message: 'Admission request not found.' });
      let parent = auth.db.parents.find(p => p.school_id === auth.user.school_id && (normalizeEmail(p.email) && normalizeEmail(p.email) === normalizeEmail(body.parentEmail || admission.email) || (safeText(p.phone) && safeText(p.phone) === safeText(body.parentPhone || admission.phone))));
      if (!parent) {
        parent = {
          id: nextId(auth.db, 'parents'),
          school_id: auth.user.school_id,
          name: safeText(body.parentName) || admission.father_name || 'Parent',
          phone: safeText(body.parentPhone) || admission.phone,
          email: normalizeEmail(body.parentEmail || admission.email),
          address: safeText(body.address) || admission.address,
          created_at: nowIso()
        };
        auth.db.parents.push(parent);
      }
      const student = {
        id: nextId(auth.db, 'students'),
        school_id: auth.user.school_id,
        parent_id: parent.id,
        roll_no: safeText(body.rollNo) || `${admission.class_name || 'A'}-${admission.id}`,
        name: admission.student_name,
        class_name: safeText(body.className) || admission.class_name,
        section: safeText(body.section) || 'A',
        father_name: admission.father_name,
        phone: admission.phone,
        address: admission.address,
        dob: admission.dob,
        created_at: nowIso()
      };
      auth.db.students.push(student);

      const parentIdentity = {
        email: normalizeEmail(body.parentEmail) || generatePortalIdentity('parent', admission.school_code, parent.id).email,
        password: safeText(body.parentPassword) || generatePortalIdentity('parent', admission.school_code, parent.id).password
      };
      const studentIdentity = {
        email: normalizeEmail(body.studentEmail) || generatePortalIdentity('student', admission.school_code, student.id).email,
        password: safeText(body.studentPassword) || generatePortalIdentity('student', admission.school_code, student.id).password
      };
      if (!auth.db.users.find(u => u.email === parentIdentity.email)) {
        auth.db.users.push({
          id: nextId(auth.db, 'users'), school_id: auth.user.school_id, role: 'parent', name: parent.name, email: parentIdentity.email,
          password_hash: hashPassword(parentIdentity.password), linked_parent_id: parent.id, linked_student_id: null, is_active: true, created_at: nowIso()
        });
      }
      if (!auth.db.users.find(u => u.email === studentIdentity.email)) {
        auth.db.users.push({
          id: nextId(auth.db, 'users'), school_id: auth.user.school_id, role: 'student', name: student.name, email: studentIdentity.email,
          password_hash: hashPassword(studentIdentity.password), linked_parent_id: null, linked_student_id: student.id, is_active: true, created_at: nowIso()
        });
      }
      admission.status = 'approved';
      admission.approved_at = nowIso();
      admission.approved_by = auth.user.id;
      admission.generated_student_id = student.id;
      admission.generated_parent_id = parent.id;
      admission.generated_roll_no = student.roll_no;
      admission.generated_section = student.section;
      admission.admission_fee = Number(body.admissionFee || 0);
      admission.parent_portal_email = parentIdentity.email;
      admission.parent_portal_password = parentIdentity.password;
      admission.student_portal_email = studentIdentity.email;
      admission.student_portal_password = studentIdentity.password;
      writeDb(auth.db);
      return json(res, 201, { message: 'Admission approved, student created and printable slip generated.', slip_url: `/admission-slip/${admission.id}` });
    }

    const updateStudentMatch = pathname.match(/^\/api\/admin\/students\/(\d+)$/);
    if (req.method === 'PATCH' && updateStudentMatch) {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(updateStudentMatch[1]) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      student.name = safeText(body.name) || student.name;
      student.roll_no = safeText(body.rollNo) || student.roll_no;
      student.class_name = safeText(body.className) || student.class_name;
      student.section = safeText(body.section) || student.section;
      student.father_name = safeText(body.fatherName) || student.father_name;
      student.phone = safeText(body.phone) || student.phone;
      student.address = safeText(body.address) || student.address;
      if (body.parentId) student.parent_id = Number(body.parentId);
      const studentUser = auth.db.users.find(u => u.linked_student_id === student.id && u.school_id === auth.user.school_id);
      if (studentUser) studentUser.name = student.name;
      writeDb(auth.db);
      return json(res, 200, { message: 'Student profile updated successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/teachers') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const email = normalizeEmail(body.email);
      if (email && auth.db.users.some(u => u.email === email)) return json(res, 409, { message: 'Teacher email already exists.' });
      const teacher = {
        id: nextId(auth.db, 'teachers'),
        school_id: auth.user.school_id,
        name: safeText(body.name),
        subject: safeText(body.subject),
        assigned_class: safeText(body.assignedClass),
        phone: safeText(body.phone),
        email,
        created_at: nowIso()
      };
      auth.db.teachers.push(teacher);
      if (email && body.portalPassword) {
        auth.db.users.push({
          id: nextId(auth.db, 'users'),
          school_id: auth.user.school_id,
          role: 'teacher',
          name: teacher.name,
          email,
          password_hash: hashPassword(body.portalPassword),
          linked_parent_id: null,
          linked_student_id: null,
          linked_teacher_id: teacher.id,
          is_active: true,
          created_at: nowIso()
        });
      }
      writeDb(auth.db);
      return json(res, 201, { message: 'Teacher added successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/salaries/assign') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const teacher = auth.db.teachers.find(t => t.id === Number(body.teacherId) && t.school_id === auth.user.school_id);
      if (!teacher) return json(res, 404, { message: 'Teacher not found.' });
      const grossAmount = Number(body.amount || body.grossAmount || 0);
      const bonusAmount = Number(body.bonusAmount || 0);
      const deductionAmount = Number(body.deductionAmount || 0);
      const netAmount = Math.max(0, grossAmount + bonusAmount - deductionAmount);
      auth.db.salaries.push({
        id: nextId(auth.db, 'salaries'),
        school_id: auth.user.school_id,
        teacher_id: teacher.id,
        title: safeText(body.title) || 'Monthly Salary',
        month_label: safeText(body.monthLabel),
        gross_amount: grossAmount,
        bonus_amount: bonusAmount,
        deduction_amount: deductionAmount,
        net_amount: netAmount,
        amount_paid: 0,
        balance_due: netAmount,
        due_date: safeText(body.dueDate),
        note: safeText(body.note),
        status: 'unpaid',
        created_by: auth.user.id,
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Teacher salary assigned successfully.' });
    }

    const salaryPaidMatch = pathname.match(/^\/api\/admin\/salaries\/(\d+)\/mark-paid$/);
    if (req.method === 'POST' && salaryPaidMatch) {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const salary = auth.db.salaries.find(s => s.id === Number(salaryPaidMatch[1]) && s.school_id === auth.user.school_id);
      if (!salary) return json(res, 404, { message: 'Salary record not found.' });
      const derived = deriveSalaryState(auth.db, salary);
      const requestedAmount = Number(body.amount || derived.balance_due || 0);
      if (requestedAmount <= 0) return json(res, 400, { message: 'Please enter a valid salary payment amount.' });
      if (requestedAmount > derived.balance_due) return json(res, 400, { message: 'Salary payment cannot be greater than the remaining balance.' });
      const remaining = Math.max(0, derived.balance_due - requestedAmount);
      salary.gross_amount = derived.gross_amount;
      salary.bonus_amount = derived.bonus_amount;
      salary.deduction_amount = derived.deduction_amount;
      salary.net_amount = derived.net_amount;
      salary.amount_paid = Number(derived.amount_paid || 0) + requestedAmount;
      salary.balance_due = remaining;
      salary.status = remaining === 0 ? 'paid' : 'partial';
      auth.db.salaryPayments.push({
        id: nextId(auth.db, 'salaryPayments'),
        school_id: auth.user.school_id,
        salary_id: salary.id,
        amount: requestedAmount,
        previous_balance: derived.balance_due,
        remaining_balance: remaining,
        payment_type: safeText(body.paymentType) || (remaining === 0 ? 'full' : 'partial'),
        method: safeText(body.method) || 'Cash',
        note: safeText(body.note),
        status: 'paid',
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: remaining === 0 ? 'Salary fully paid.' : 'Partial salary payment saved and remaining balance updated.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/fees/assign') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      auth.db.fees.push({
        id: nextId(auth.db, 'fees'),
        school_id: auth.user.school_id,
        student_id: student.id,
        title: safeText(body.title),
        month_label: safeText(body.monthLabel),
        amount: Number(body.amount || 0),
        due_date: safeText(body.dueDate),
        notes: safeText(body.notes),
        status: 'unpaid',
        original_amount: Number(body.amount || 0),
        balance_due: Number(body.amount || 0),
        amount_paid: 0,
        created_by: auth.user.id,
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Fee assigned successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/fees/bulk-assign') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const students = auth.db.students.filter(s => s.school_id === auth.user.school_id && (body.className === 'all' || safeText(s.class_name) === safeText(body.className)));
      if (!students.length) return json(res, 404, { message: 'No students found for this class filter.' });
      students.forEach(student => {
        auth.db.fees.push({
          id: nextId(auth.db, 'fees'),
          school_id: auth.user.school_id,
          student_id: student.id,
          title: safeText(body.title),
          month_label: safeText(body.monthLabel),
          amount: Number(body.amount || 0),
          due_date: safeText(body.dueDate),
          notes: safeText(body.notes),
          status: 'unpaid',
          original_amount: Number(body.amount || 0),
          balance_due: Number(body.amount || 0),
          amount_paid: 0,
          created_by: auth.user.id,
          created_at: nowIso()
        });
      });
      writeDb(auth.db);
      return json(res, 201, { message: `Bulk fee assigned to ${students.length} students.` });
    }

    const markPaidMatch = pathname.match(/^\/api\/admin\/fees\/(\d+)\/mark-paid$/);
    if (req.method === 'POST' && markPaidMatch) {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const fee = auth.db.fees.find(f => f.id === Number(markPaidMatch[1]) && f.school_id === auth.user.school_id);
      if (!fee) return json(res, 404, { message: 'Fee not found.' });
      const derived = deriveFeeState(auth.db, fee);
      const requestedAmount = Number(body.amount || derived.balance_due || fee.amount || 0);
      if (requestedAmount <= 0) return json(res, 400, { message: 'Amount must be greater than zero.' });
      if (requestedAmount > derived.balance_due) return json(res, 400, { message: 'Amount cannot be greater than remaining balance.' });
      const remaining = Math.max(0, derived.balance_due - requestedAmount);
      fee.original_amount = derived.original_amount;
      fee.amount_paid = Number(derived.amount_paid || 0) + requestedAmount;
      fee.balance_due = remaining;
      fee.amount = remaining;
      fee.status = remaining === 0 ? 'paid' : 'partial';
      const payment = {
        id: nextId(auth.db, 'payments'),
        school_id: auth.user.school_id,
        fee_id: fee.id,
        amount: requestedAmount,
        payment_type: safeText(body.paymentType) || (remaining === 0 ? 'full' : 'partial'),
        previous_balance: derived.balance_due,
        remaining_balance: remaining,
        method: safeText(body.method) || 'Cash',
        reference_no: safeText(body.referenceNo),
        note: safeText(body.note),
        status: 'paid',
        created_at: nowIso()
      };
      auth.db.payments.push(payment);
      writeDb(auth.db);
      return json(res, 201, { message: remaining === 0 ? 'Fee fully paid and receipt generated.' : 'Partial payment saved and remaining balance updated.', receipt_url: buildReceiptUrl(payment.id) });
    }

    if (req.method === 'POST' && pathname === '/api/admin/attendance/mark') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      auth.db.attendance.push({
        id: nextId(auth.db, 'attendance'),
        school_id: auth.user.school_id,
        student_id: student.id,
        attendance_date: safeText(body.attendanceDate),
        status: safeText(body.status) || 'present',
        note: safeText(body.note),
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Attendance saved successfully.' });
    }


    if (req.method === 'POST' && pathname === '/api/admin/results/batch') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      const examTitle = safeText(body.examTitle);
      const originalExamTitle = safeText(body.originalExamTitle) || examTitle;
      const rows = String(body.subjectRows || '').split(/\r?\n/).map(x => x.trim()).filter(Boolean);
      if (!rows.length) return json(res, 400, { message: 'Please enter at least one subject row.' });
      auth.db.results = auth.db.results.filter(r => !(r.school_id === auth.user.school_id && r.student_id === student.id && [examTitle, originalExamTitle].includes(safeText(r.exam_title))));
      let created = 0;
      const positionValue = safeText(body.positionValue);
      const customPosition = safeText(body.customPosition);
      const positionLabel = customPosition || ({ first: '1st Position', second: '2nd Position', third: '3rd Position', overall: 'Overall Result', custom: 'Custom Position' }[positionValue] || safeText(positionValue));
      for (const row of rows) {
        const [subject, totalMarks, obtainedMarks, remarks] = row.split('|').map(v => safeText(v));
        if (!subject) continue;
        const total = Number(totalMarks || 100);
        const obtained = Number(obtainedMarks || 0);
        auth.db.results.push({
          id: nextId(auth.db, 'results'),
          school_id: auth.user.school_id,
          student_id: student.id,
          exam_title: examTitle,
          subject,
          total_marks: total,
          obtained_marks: obtained,
          grade: computeGrade(obtained, total),
          remarks,
          overall_remarks: safeText(body.overallRemarks),
          result_status: safeText(body.resultStatus),
          position_value: positionValue,
          position_label: positionLabel,
          created_at: nowIso()
        });
        created += 1;
      }
      writeDb(auth.db);
      return json(res, 201, { message: `${created} subject results saved successfully. Existing rows for this exam were updated.` });
    }

    if (req.method === 'POST' && pathname === '/api/admin/results') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      auth.db.results.push({
        id: nextId(auth.db, 'results'),
        school_id: auth.user.school_id,
        student_id: student.id,
        exam_title: safeText(body.examTitle),
        subject: safeText(body.subject),
        total_marks: Number(body.totalMarks || 0),
        obtained_marks: Number(body.obtainedMarks || 0),
        grade: safeText(body.grade) || computeGrade(Number(body.obtainedMarks || 0), Number(body.totalMarks || 0)),
        remarks: safeText(body.remarks),
        overall_remarks: safeText(body.overallRemarks),
        result_status: safeText(body.resultStatus),
        position_value: safeText(body.positionValue),
        position_label: safeText(body.customPosition) || ({ first: '1st Position', second: '2nd Position', third: '3rd Position', overall: 'Overall Result' }[safeText(body.positionValue)] || safeText(body.positionValue)),
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Result added successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/sync/students-from-sheet') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      if (!body.csvUrl) return json(res, 400, { message: 'CSV URL is required.' });
      const rows = await parsePublicCsv(body.csvUrl);
      let imported = 0;
      rows.forEach(row => {
        const name = safeText(row.name || row.student_name || row.student || row.full_name);
        if (!name) return;

        const rollNo = safeText(row.roll_no || row.roll || row.roll_number || row.admission_no || row.admission_number);
        const parentName = safeText(row.parent_name || row.father_name || row.guardian_name || '');
        let parentId = null;

        if (parentName) {
          let parent = auth.db.parents.find(p => p.school_id === auth.user.school_id && p.name.toLowerCase() === parentName.toLowerCase());
          if (!parent) {
            parent = {
              id: nextId(auth.db, 'parents'),
              school_id: auth.user.school_id,
              name: parentName,
              phone: safeText(row.parent_phone || row.guardian_phone || row.parent_mobile || row.phone),
              email: normalizeEmail(row.parent_email || row.guardian_email),
              address: safeText(row.address),
              created_at: nowIso()
            };
            auth.db.parents.push(parent);
          }
          parentId = parent.id;
        }

        const duplicate = auth.db.students.find(s => s.school_id === auth.user.school_id && ((rollNo && s.roll_no === rollNo) || (!rollNo && s.name.toLowerCase() === name.toLowerCase())));
        if (duplicate) return;

        auth.db.students.push({
          id: nextId(auth.db, 'students'),
          school_id: auth.user.school_id,
          parent_id: parentId,
          roll_no: rollNo,
          name,
          class_name: safeText(row.class_name || row.class || row.grade),
          section: safeText(row.section),
          father_name: safeText(row.father_name || row.guardian_name || parentName),
          phone: safeText(row.phone || row.student_phone || row.mobile),
          address: safeText(row.address),
          created_at: nowIso()
        });
        imported += 1;
      });
      writeDb(auth.db);
      return json(res, 201, { message: `${imported} students imported successfully.` });
    }

    if (req.method === 'PATCH' && pathname === '/api/admin/school-settings') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const body = await parseBody(req);
      const school = auth.db.schools.find(s => s.id === auth.user.school_id);
      if (!school) return json(res, 404, { message: 'School not found.' });
      school.name = safeText(body.name) || school.name;
      school.tagline = safeText(body.tagline);
      school.address = safeText(body.address);
      school.phone = safeText(body.phone);
      school.logo_url = chooseLogoValue(body, school.logo_url);
      school.primary_color = safeText(body.primaryColor) || school.primary_color || '#2146d0';
      school.whatsapp_number = safeText(body.whatsappNumber);
      writeDb(auth.db);
      return json(res, 200, { message: 'School settings updated successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/admin/notifications/generate-fee-alerts') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const scoped = getSchoolScoped(auth.db, auth.user.school_id);
      const unpaidFees = scoped.fees.filter(f => deriveFeeState(auth.db, f).balance_due > 0);
      let created = 0;
      unpaidFees.forEach(fee => {
        const related = feeWithRelations(auth.db, fee);
        const state = deriveFeeState(auth.db, fee);
        const message = `Hello. The remaining balance for ${related.student_name} on ${fee.title} (${fee.month_label || ''}) is Rs. ${Number(state.balance_due || 0).toLocaleString()}. Due date: ${fee.due_date || ''}. Fee ID: ${fee.id}.`;
        auth.db.notifications.push({
          id: nextId(auth.db, 'notifications'),
          school_id: auth.user.school_id,
          target_type: 'parent',
          target_name: related.parent_name,
          phone: related.parent_phone,
          channel: 'WhatsApp/SMS template',
          message,
          wa_link: related.parent_phone ? `https://wa.me/${String(related.parent_phone).replace(/\D/g, '')}?text=${encodeURIComponent(message)}` : '',
          created_at: nowIso()
        });
        created += 1;
      });
      writeDb(auth.db);
      return json(res, 201, { message: `${created} fee reminder templates generated.` });
    }

    if (req.method === 'GET' && pathname === '/api/teacher/overview') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const teacher = auth.user.linked_teacher_id ? auth.db.teachers.find(t => t.id === auth.user.linked_teacher_id) : auth.db.teachers.find(t => t.email === auth.user.email && t.school_id === auth.user.school_id);
      const scoped = getSchoolScoped(auth.db, auth.user.school_id);
      const salaryTotals = teacher ? getTeacherSalaryTotals(auth.db, teacher.id) : { total: 0, paid: 0, pending: 0 };
      return json(res, 200, {
        teacher,
        students: scoped.students.length,
        attendance: scoped.attendance.length,
        results: scoped.results.length,
        notices: scoped.notices.length,
        salary_total: salaryTotals.total,
        salary_paid: salaryTotals.paid,
        salary_pending: salaryTotals.pending
      });
    }

    if (req.method === 'GET' && pathname === '/api/teacher/profile') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const teacher = auth.user.linked_teacher_id ? auth.db.teachers.find(t => t.id === auth.user.linked_teacher_id && t.school_id === auth.user.school_id) : auth.db.teachers.find(t => t.email === auth.user.email && t.school_id === auth.user.school_id);
      if (!teacher) return json(res, 404, { message: 'Teacher profile not found.' });
      const assignedStudents = auth.db.students.filter(s => s.school_id === auth.user.school_id && (safeText(teacher.assigned_class) === safeText(`${s.class_name}-${s.section}`) || safeText(teacher.assigned_class) === safeText(s.class_name)));
      const salaryTotals = getTeacherSalaryTotals(auth.db, teacher.id);
      return json(res, 200, { ...teacher, assigned_students: assignedStudents.length, salary_total: salaryTotals.total, salary_paid: salaryTotals.paid, salary_pending: salaryTotals.pending, school: auth.db.schools.find(s => s.id === auth.user.school_id) || null });
    }

    if (req.method === 'GET' && pathname === '/api/teacher/students') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).students);
    }

    if (req.method === 'GET' && pathname === '/api/teacher/attendance') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).attendance.map(a => {
        const student = auth.db.students.find(s => s.id === a.student_id);
        return { ...a, student_name: student?.name || '', class_name: student?.class_name || '', section: student?.section || '' };
      }));
    }

    if (req.method === 'GET' && pathname === '/api/teacher/results') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).results.map(r => ({
        ...r,
        student_name: auth.db.students.find(s => s.id === r.student_id)?.name || '',
        result_card_url: buildResultCardUrl(r.student_id, r.exam_title)
      })));
    }

    if (req.method === 'GET' && pathname === '/api/teacher/notices') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      return json(res, 200, getSchoolScoped(auth.db, auth.user.school_id).notices.slice().reverse());
    }

    if (req.method === 'GET' && pathname === '/api/teacher/salaries') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const teacher = auth.user.linked_teacher_id ? auth.db.teachers.find(t => t.id === auth.user.linked_teacher_id && t.school_id === auth.user.school_id) : auth.db.teachers.find(t => t.email === auth.user.email && t.school_id === auth.user.school_id);
      if (!teacher) return json(res, 404, { message: 'Teacher profile not found.' });
      const rows = auth.db.salaries.filter(s => s.teacher_id === teacher.id && s.school_id === auth.user.school_id).map(salary => ({
        ...salaryWithRelations(auth.db, salary),
        slip_url: buildSalarySlipUrl(salary.id)
      }));
      return json(res, 200, rows);
    }

    if (req.method === 'GET' && pathname === '/api/teacher/salary-payments') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const teacher = auth.user.linked_teacher_id ? auth.db.teachers.find(t => t.id === auth.user.linked_teacher_id && t.school_id === auth.user.school_id) : auth.db.teachers.find(t => t.email === auth.user.email && t.school_id === auth.user.school_id);
      if (!teacher) return json(res, 404, { message: 'Teacher profile not found.' });
      const salaryIds = auth.db.salaries.filter(s => s.teacher_id === teacher.id && s.school_id === auth.user.school_id).map(s => s.id);
      const rows = auth.db.salaryPayments.filter(p => p.school_id === auth.user.school_id && salaryIds.includes(p.salary_id)).map(payment => {
        const salary = auth.db.salaries.find(s => s.id === payment.salary_id);
        return {
          ...payment,
          salary_title: salary?.title || 'Salary',
          month_label: salary?.month_label || '',
          slip_url: salary ? buildSalarySlipUrl(salary.id) : ''
        };
      });
      return json(res, 200, rows);
    }

    if (req.method === 'GET' && pathname === '/api/teacher/yearly-salary-report') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const teacher = auth.user.linked_teacher_id ? auth.db.teachers.find(t => t.id === auth.user.linked_teacher_id && t.school_id === auth.user.school_id) : auth.db.teachers.find(t => t.email === auth.user.email && t.school_id === auth.user.school_id);
      if (!teacher) return json(res, 404, { message: 'Teacher profile not found.' });
      const summary = getPayrollSummary(auth.db, auth.user.school_id);
      return json(res, 200, summary.teachers.filter(item => item.teacher_id === teacher.id));
    }

    if (req.method === 'POST' && pathname === '/api/teacher/attendance/mark') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      auth.db.attendance.push({
        id: nextId(auth.db, 'attendance'),
        school_id: auth.user.school_id,
        student_id: student.id,
        attendance_date: safeText(body.attendanceDate),
        status: safeText(body.status) || 'present',
        note: safeText(body.note),
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Attendance saved successfully.' });
    }

    if (req.method === 'POST' && pathname === '/api/teacher/results') {
      const auth = requireAuth(req, res, ['teacher']);
      if (!auth) return;
      const body = await parseBody(req);
      const student = auth.db.students.find(s => s.id === Number(body.studentId) && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student not found.' });
      auth.db.results.push({
        id: nextId(auth.db, 'results'),
        school_id: auth.user.school_id,
        student_id: student.id,
        exam_title: safeText(body.examTitle),
        subject: safeText(body.subject),
        total_marks: Number(body.totalMarks || 0),
        obtained_marks: Number(body.obtainedMarks || 0),
        grade: safeText(body.grade) || computeGrade(Number(body.obtainedMarks || 0), Number(body.totalMarks || 0)),
        remarks: safeText(body.remarks),
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Result added successfully.' });
    }

    if (req.method === 'GET' && pathname === '/api/parent/children') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const items = auth.db.students.filter(s => s.school_id === auth.user.school_id && s.parent_id === auth.user.linked_parent_id).map(student => {
        const feeTotals = getStudentFeeTotals(auth.db, student.id);
        const attendance = getStudentAttendanceSummary(auth.db, student.id);
        const resultCount = auth.db.results.filter(r => r.student_id === student.id && r.school_id === auth.user.school_id).length;
        return { ...student, pending_fee: feeTotals.pending, total_paid: feeTotals.paid, result_count: resultCount, attendance_present: attendance.present, attendance_absent: attendance.absent };
      });
      return json(res, 200, items);
    }

    if (req.method === 'GET' && pathname === '/api/parent/profile') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const parent = auth.db.parents.find(p => p.id === auth.user.linked_parent_id && p.school_id === auth.user.school_id);
      if (!parent) return json(res, 404, { message: 'Parent profile not found.' });
      const summary = getParentSummary(auth.db, parent.id, auth.user.school_id);
      return json(res, 200, { ...parent, ...summary, school: auth.db.schools.find(s => s.id === auth.user.school_id) || null });
    }

    if (req.method === 'GET' && pathname === '/api/parent/fees') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const childrenIds = auth.db.students.filter(s => s.school_id === auth.user.school_id && s.parent_id === auth.user.linked_parent_id).map(s => s.id);
      const fees = auth.db.fees.filter(f => f.school_id === auth.user.school_id && childrenIds.includes(f.student_id)).map(f => {
        const related = feeWithRelations(auth.db, f);
        return {
          ...related,
          ...buildFeeShareLinks(req, f, related.parent_phone, related.student_name)
        };
      });
      return json(res, 200, fees);
    }

    if (req.method === 'GET' && pathname === '/api/parent/results') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const childrenIds = auth.db.students.filter(s => s.school_id === auth.user.school_id && s.parent_id === auth.user.linked_parent_id).map(s => s.id);
      const results = auth.db.results.filter(r => r.school_id === auth.user.school_id && childrenIds.includes(r.student_id)).map(r => ({
        ...r,
        student_name: auth.db.students.find(s => s.id === r.student_id)?.name || '',
        result_card_url: buildResultCardUrl(r.student_id, r.exam_title)
      }));
      return json(res, 200, results);
    }

    if (req.method === 'GET' && pathname === '/api/parent/payments') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const childrenIds = auth.db.students.filter(s => s.school_id === auth.user.school_id && s.parent_id === auth.user.linked_parent_id).map(s => s.id);
      const feeIds = auth.db.fees.filter(f => f.school_id === auth.user.school_id && childrenIds.includes(f.student_id)).map(f => f.id);
      const payments = auth.db.payments.filter(p => p.school_id === auth.user.school_id && feeIds.includes(p.fee_id)).map(p => ({
        ...paymentWithRelations(auth.db, p),
        receipt_url: buildReceiptUrl(p.id)
      }));
      return json(res, 200, payments);
    }

    if (req.method === 'GET' && pathname === '/api/parent/notices') {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      return json(res, 200, auth.db.notices.filter(n => n.school_id === auth.user.school_id && ['all', 'parents'].includes(String(n.audience || 'all'))).slice().reverse());
    }

    const submitPaymentMatch = pathname.match(/^\/api\/parent\/fees\/(\d+)\/submit-payment$/);
    if (req.method === 'POST' && submitPaymentMatch) {
      const auth = requireAuth(req, res, ['parent']);
      if (!auth) return;
      const body = await parseBody(req);
      const fee = auth.db.fees.find(f => f.id === Number(submitPaymentMatch[1]) && f.school_id === auth.user.school_id);
      if (!fee) return json(res, 404, { message: 'Fee not found.' });
      const student = auth.db.students.find(s => s.id === fee.student_id);
      if (!student || student.parent_id !== auth.user.linked_parent_id) return json(res, 403, { message: 'This fee does not belong to your child.' });
      auth.db.payments.push({
        id: nextId(auth.db, 'payments'),
        school_id: auth.user.school_id,
        fee_id: fee.id,
        amount: Number(body.amount || fee.amount || 0),
        method: safeText(body.method) || 'Bank Transfer',
        reference_no: safeText(body.referenceNo),
        note: safeText(body.note),
        status: 'submitted',
        created_at: nowIso()
      });
      writeDb(auth.db);
      return json(res, 201, { message: 'Payment submitted for admin verification.' });
    }

    if (req.method === 'GET' && pathname === '/api/student/profile') {
      const auth = requireAuth(req, res, ['student']);
      if (!auth) return;
      const student = auth.db.students.find(s => s.id === auth.user.linked_student_id && s.school_id === auth.user.school_id);
      if (!student) return json(res, 404, { message: 'Student profile not found.' });
      const parent = auth.db.parents.find(p => p.id === student.parent_id);
      const feeTotals = getStudentFeeTotals(auth.db, student.id);
      const attendance = getStudentAttendanceSummary(auth.db, student.id);
      return json(res, 200, { ...student, parent_name: parent?.name || '', parent_phone: parent?.phone || '', parent_email: parent?.email || '', total_fee: feeTotals.total, total_paid: feeTotals.paid, pending_fee: feeTotals.pending, attendance_present: attendance.present, attendance_absent: attendance.absent, attendance_total: attendance.total });
    }

    if (req.method === 'GET' && pathname === '/api/student/fees') {
      const auth = requireAuth(req, res, ['student']);
      if (!auth) return;
      const fees = auth.db.fees.filter(f => f.student_id === auth.user.linked_student_id && f.school_id === auth.user.school_id).map(f => {
        const related = feeWithRelations(auth.db, f);
        return {
          ...f,
          ...buildFeeShareLinks(req, f, related.parent_phone, related.student_name)
        };
      });
      return json(res, 200, fees);
    }

    if (req.method === 'GET' && pathname === '/api/student/results') {
      const auth = requireAuth(req, res, ['student']);
      if (!auth) return;
      return json(res, 200, auth.db.results.filter(r => r.student_id === auth.user.linked_student_id && r.school_id === auth.user.school_id).map(r => ({
        ...r,
        result_card_url: buildResultCardUrl(r.student_id, r.exam_title)
      })));
    }

    if (req.method === 'GET' && pathname === '/api/student/notices') {
      const auth = requireAuth(req, res, ['student']);
      if (!auth) return;
      return json(res, 200, auth.db.notices.filter(n => n.school_id === auth.user.school_id && ['all', 'students'].includes(String(n.audience || 'all'))).slice().reverse());
    }

    if (req.method === 'GET' && pathname === '/api/lookups/classes') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      const classes = [...new Set(auth.db.students.filter(s => s.school_id === auth.user.school_id).map(s => s.class_name).filter(Boolean))].sort();
      return json(res, 200, classes);
    }

    if (req.method === 'GET' && pathname === '/api/lookups/students') {
      const auth = requireAuth(req, res, ['admin']);
      if (!auth) return;
      return json(res, 200, auth.db.students.filter(s => s.school_id === auth.user.school_id));
    }


    const admissionSlipMatch = pathname.match(/^\/admission-slip\/(\d+)$/);
    if (req.method === 'GET' && admissionSlipMatch) {
      const db = readDb();
      const admission = db.admissionRequests.find(a => a.id === Number(admissionSlipMatch[1]));
      if (!admission) return sendHtml(res, 404, '<h1>Admission slip not found</h1>');
      return sendHtml(res, 200, renderAdmissionSlipHtml(db, admission));
    }

    const receiptMatch = pathname.match(/^\/receipt\/(\d+)$/);
    if (req.method === 'GET' && receiptMatch) {
      const db = readDb();
      const payment = db.payments.find(p => p.id === Number(receiptMatch[1]));
      if (!payment) return sendHtml(res, 404, '<h1>Receipt not found</h1>');
      return sendHtml(res, 200, renderReceiptHtml(db, payment));
    }

    const challanMatch = pathname.match(/^\/challan\/(\d+)$/);
    if (req.method === 'GET' && challanMatch) {
      const db = readDb();
      const fee = db.fees.find(f => f.id === Number(challanMatch[1]));
      if (!fee) return sendHtml(res, 404, '<h1>Challan not found</h1>');
      return sendHtml(res, 200, renderChallanHtml(db, fee, req));
    }

    const salarySlipMatch = pathname.match(/^\/salary-slip\/(\d+)$/);
    if (req.method === 'GET' && salarySlipMatch) {
      const db = readDb();
      const salary = db.salaries.find(s => s.id === Number(salarySlipMatch[1]));
      if (!salary) return sendHtml(res, 404, '<h1>Salary slip not found</h1>');
      return sendHtml(res, 200, renderSalarySlipHtml(db, salary));
    }

    const resultCardMatch = pathname.match(/^\/result-card\/(\d+)$/);
    if (req.method === 'GET' && resultCardMatch) {
      const db = readDb();
      const exam = fullUrl.searchParams.get('exam') || '';
      return sendHtml(res, 200, renderResultCardHtml(db, Number(resultCardMatch[1]), exam));
    }


    if (req.method === 'GET' && pathname === '/payroll-sheet') {
      const db = readDb();
      const month = safeText(fullUrl.searchParams.get('month'));
      const schoolCode = safeText(fullUrl.searchParams.get('school'));
      const school = schoolCode ? schoolByCode(db, schoolCode) : db.schools[0];
      if (!school) return sendHtml(res, 404, '<h1>School not found</h1>');
      return sendHtml(res, 200, renderPayrollSheetHtml(db, school.id, month));
    }

    if (pathname.startsWith('/api/')) return json(res, 404, { message: 'API route not found.' });
    if (pathname === '/') return serveStatic(req, res, '/index.html');
    return serveStatic(req, res, pathname);
  } catch (error) {
    console.error(error);
    if (!res.headersSent) return json(res, 500, { message: error.message || 'Server error.' });
    res.end();
  }
});

server.listen(PORT, () => {
  console.log(`Private School Portal running on http://localhost:${PORT}`);
});
