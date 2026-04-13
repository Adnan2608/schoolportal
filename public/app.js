const state = {
  token: localStorage.getItem('portal_token') || '',
  user: JSON.parse(localStorage.getItem('portal_user') || 'null'),
  initialized: false,
  ownerSchools: [],
  publicSchools: [],
  adminStudents: [],
  adminParents: [],
  adminTeachers: [],
  adminSalaries: [],
  adminResults: [],
  teacherSalaries: []
};

const $ = (id) => document.getElementById(id);

function showMessage(id, text = '', isError = false) {
  const el = $(id);
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? 'var(--danger)' : 'var(--success)';
}

async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const response = await fetch(path, { ...options, headers });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.message || 'Request failed');
  return data;
}

function saveSession(token, user) {
  state.token = token;
  state.user = user;
  localStorage.setItem('portal_token', token);
  localStorage.setItem('portal_user', JSON.stringify(user));
}

function clearSession() {
  state.token = '';
  state.user = null;
  localStorage.removeItem('portal_token');
  localStorage.removeItem('portal_user');
}

function currency(value) {
  return `Rs. ${Number(value || 0).toLocaleString()}`;
}

function badge(status) {
  const clean = String(status || '').toLowerCase();
  return `<span class="badge ${clean}">${status || ''}</span>`;
}

function linkHtml(url, label) {
  return url ? `<a href="${url}" target="_blank" rel="noopener">${label}</a>` : '';
}

function setTable(id, headers, rows) {
  const el = $(id);
  if (!el) return;
  const head = `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const body = rows.length
    ? `<tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell ?? ''}</td>`).join('')}</tr>`).join('')}</tbody>`
    : `<tbody><tr><td colspan="${headers.length}">No records found</td></tr></tbody>`;
  el.innerHTML = head + body;
}

function renderStats(id, items) {
  const el = $(id);
  if (!el) return;
  el.innerHTML = items.map(item => `
    <div class="stat">
      <div class="label">${item.label}</div>
      <div class="value">${item.value}</div>
    </div>
  `).join('');
}

function setBranding(school) {
  if (!school) return;
  const root = document.documentElement;
  if (school.primary_color) root.style.setProperty('--primary', school.primary_color);
  $('brandTitle').textContent = school.name || 'Private School Management Portal';
  $('brandSubtitle').textContent = school.tagline || 'Private School Management Portal';
}

function setViewState() {
  const loggedIn = !!state.token && !!state.user;
  $('setupView').classList.toggle('hidden', state.initialized || loggedIn);
  $('publicView').classList.toggle('hidden', !state.initialized || loggedIn);
  $('resetCard').classList.toggle('hidden', true);
  $('appView').classList.toggle('hidden', !loggedIn);
  $('logoutBtn').classList.toggle('hidden', !loggedIn);
  $('ownerPanel').classList.add('hidden');
  $('adminPanel').classList.add('hidden');
  $('teacherPanel').classList.add('hidden');
  $('parentPanel').classList.add('hidden');
  $('studentPanel').classList.add('hidden');

  if (!loggedIn) {
    $('sessionInfo').textContent = state.initialized ? 'Not logged in' : 'Setup required';
    return;
  }

  const schoolText = state.user.school?.name ? ` • ${state.user.school.name}` : '';
  $('sessionInfo').textContent = `${state.user.name} (${state.user.role})${schoolText}`;
  if (state.user.school) setBranding(state.user.school);
  if (state.user.role === 'owner') $('ownerPanel').classList.remove('hidden');
  if (state.user.role === 'admin') $('adminPanel').classList.remove('hidden');
  if (state.user.role === 'teacher') $('teacherPanel').classList.remove('hidden');
  if (state.user.role === 'parent') $('parentPanel').classList.remove('hidden');
  if (state.user.role === 'student') $('studentPanel').classList.remove('hidden');
}

function formDataObject(form) {
  return Object.fromEntries(new FormData(form).entries());
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    if (!file) return resolve('');
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(new Error('Could not read the selected file.'));
    reader.readAsDataURL(file);
  });
}

async function collectFormDataWithLogo(form) {
  const body = formDataObject(form);
  const fileInput = form.querySelector('input[name="logoFile"]');
  const file = fileInput && fileInput.files ? fileInput.files[0] : null;
  if (file) body.logoDataUrl = await fileToDataUrl(file);
  return body;
}

function fillSelect(selectId, options, placeholder = '') {
  const el = $(selectId);
  if (!el) return;
  let html = placeholder ? `<option value="">${placeholder}</option>` : '';
  html += options.join('');
  el.innerHTML = html;
}

function populatePublicSchools() {
  fillSelect('admissionSchoolSelect', state.publicSchools.map(s => `<option value="${s.name}">${s.name} (${s.code})</option>`), 'Select a school');
}

function updateStudentEditForm(studentId) {
  const student = state.adminStudents.find(s => s.id === Number(studentId));
  const form = $('studentEditForm');
  if (!student || !form) return;
  form.studentId.value = student.id;
  form.name.value = student.name || '';
  form.rollNo.value = student.roll_no || '';
  form.className.value = student.class_name || '';
  form.section.value = student.section || '';
  form.parentId.value = student.parent_id || '';
  form.fatherName.value = student.father_name || '';
  form.phone.value = student.phone || '';
  form.address.value = student.address || '';
}

function updateResultStudentPreview(studentId) {
  const student = state.adminStudents.find(s => s.id === Number(studentId));
  const box = $('resultStudentPreview');
  const subjectRows = $('subjectRowsInput');
  if (!box) return;
  if (!student) {
    box.innerHTML = 'Student details will appear here automatically.';
    return;
  }
  box.innerHTML = `<div><strong>Name:</strong> ${student.name || ''}</div><div><strong>Roll No:</strong> ${student.roll_no || ''}</div><div><strong>Class:</strong> ${student.class_name || ''}</div><div><strong>Section:</strong> ${student.section || ''}</div><div><strong>Father:</strong> ${student.father_name || ''}</div><div><strong>Phone:</strong> ${student.phone || ''}</div><div><strong>Parent Phone:</strong> ${student.parent_phone || ''}</div><div><strong>Pending Fee:</strong> ${currency(student.pending_fee || 0)}</div>`;
  if (subjectRows && !subjectRows.value.trim()) subjectRows.value = defaultSubjectRows();
}

function setResultEditState(text = '', mode = '') {
  const box = $('resultEditState');
  if (!box) return;
  box.textContent = text;
  box.classList.toggle('hidden', !text);
  box.dataset.mode = mode || '';
}

function clearResultForm(keepStudent = false) {
  const form = $('resultForm');
  if (!form) return;
  const currentStudentId = keepStudent ? form.studentId.value : '';
  form.reset();
  if (keepStudent && currentStudentId) form.studentId.value = currentStudentId;
  if (form.originalExamTitle) form.originalExamTitle.value = '';
  if ($('subjectRowsInput')) $('subjectRowsInput').value = '';
  if (form.studentId && form.studentId.value) updateResultStudentPreview(form.studentId.value);
  else if ($('resultStudentPreview')) $('resultStudentPreview').innerHTML = 'Student details will appear here automatically.';
  setResultEditState('', '');
}

function loadExistingResultIntoForm(studentId, examTitle) {
  const rows = state.adminResults.filter(r => Number(r.student_id) === Number(studentId) && String(r.exam_title || '') === String(examTitle || ''));
  const form = $('resultForm');
  if (!form || !rows.length) {
    showMessage('resultMessage', 'Could not find that saved result card.', true);
    return;
  }
  const first = rows[0];
  form.studentId.value = String(studentId);
  form.examTitle.value = first.exam_title || '';
  if (form.originalExamTitle) form.originalExamTitle.value = first.exam_title || '';
  form.resultStatus.value = first.result_status || 'Promoted';
  form.positionValue.value = first.position_value || 'overall';
  form.customPosition.value = first.position_value === 'custom' ? (first.position_label || '') : '';
  form.overallRemarks.value = first.overall_remarks || '';
  if ($('subjectRowsInput')) {
    $('subjectRowsInput').value = rows
      .sort((a, b) => Number(a.id) - Number(b.id))
      .map(r => `${r.subject || ''}|${r.total_marks || ''}|${r.obtained_marks || ''}|${r.remarks || ''}`)
      .join('\n');
  }
  updateResultStudentPreview(studentId);
  setResultEditState(`Editing saved result card: ${first.exam_title || ''} for ${first.student_name || 'selected student'}`, 'editing');
  document.getElementById('resultForm')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  showMessage('resultMessage', 'Saved result loaded into the form. Update the marks or remarks and save again.');
}

window.editResultCard = loadExistingResultIntoForm;


function defaultSubjectRows() {
  return ['Reading','Language','Spelling','Writing','Math','Science','Social Studies','Physical Education','Art','Music','Extracurricular']
    .map(subject => `${subject}|100||`)
    .join('\n');
}

function buildAttendanceSummaryRows(items) {
  const map = new Map();
  items.forEach(item => {
    const key = item.student_name || item.name || `Student ${item.student_id || ''}`;
    if (!map.has(key)) map.set(key, { present: 0, absent: 0, late: 0, total: 0, class_name: item.class_name || '', section: item.section || '' });
    const row = map.get(key);
    const status = String(item.status || '').toLowerCase();
    if (status === 'present') row.present += 1;
    if (status === 'absent') row.absent += 1;
    if (status === 'late') row.late += 1;
    row.total += 1;
  });
  return [...map.entries()].map(([name, row]) => [name, `${row.class_name || ''} ${row.section || ''}`.trim(), row.present, row.absent, row.late, row.total]);
}

function populateOwnerBrandingForm() {
  fillSelect('ownerSchoolSelect', state.ownerSchools.map(s => `<option value="${s.id}">${s.name} (${s.code})</option>`));
}

function populateSchoolBrandingPreview(school) {
  const box = $('brandPreview');
  if (!box || !school) return;
  box.innerHTML = `
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:12px;">
      ${school.logo_url ? `<img src="${school.logo_url}" alt="logo" style="width:64px;height:64px;border-radius:16px;object-fit:cover;border:1px solid #dbe4f3">` : ''}
      <div>
        <div style="font-size:20px;font-weight:800;color:${school.primary_color || '#2146d0'}">${school.name || ''}</div>
        <div>${school.tagline || ''}</div>
      </div>
    </div>
    <div><strong>Phone:</strong> ${school.phone || ''}</div>
    <div><strong>Address:</strong> ${school.address || ''}</div>
    <div><strong>WhatsApp:</strong> ${school.whatsapp_number || ''}</div>
  `;
}

async function boot() {
  try {
    const [config, schools] = await Promise.all([
      api('/api/public/config', { headers: {} }),
      api('/api/public/schools', { headers: {} })
    ]);
    state.initialized = !!config.initialized;
    state.publicSchools = schools || [];
    populatePublicSchools();
  } catch (error) {
    showMessage('loginMessage', error.message, true);
  }

  if (state.token) {
    try {
      const me = await api('/api/me');
      state.user = me.user;
    } catch {
      clearSession();
    }
  }

  setViewState();
  if (state.token && state.user) await hydrate();
}

async function loadOwner() {
  const [stats, schools, requests] = await Promise.all([
    api('/api/owner/stats'),
    api('/api/owner/schools'),
    api('/api/owner/requests')
  ]);

  state.ownerSchools = schools;
  populateOwnerBrandingForm();

  renderStats('ownerStats', [
    { label: 'Schools', value: stats.schools },
    { label: 'Students', value: stats.students },
    { label: 'Parents', value: stats.parents },
    { label: 'Teachers', value: stats.teachers },
    { label: 'Unpaid Fee', value: currency(stats.unpaid) },
    { label: 'Reset Requests', value: stats.resetRequests }
  ]);

  setTable('schoolsTable', ['ID', 'School', 'Code', 'Students', 'Parents', 'Teachers', 'Admins', 'Phone'],
    schools.map(s => [s.id, s.name, s.code, s.students_count, s.parents_count, s.teachers_count, s.admins_count, s.phone || ''])
  );

  setTable('ownerRequestsTable', ['ID', 'School', 'City', 'Contact', 'Phone', 'Email', 'Status'],
    requests.onboarding.map(r => [r.id, r.school_name, r.city || '', r.contact_name, r.phone, r.email || '', badge(r.status)])
  );

  setTable('resetRequestsTable', ['ID', 'Email', 'School Code', 'Role', 'Handled By', 'Status', 'Date'],
    requests.resets.map(r => [r.id, r.email, r.school_code || '', r.role_hint || '', r.handled_by_role || '', badge(r.status), String(r.created_at || '').slice(0, 10)])
  );
}

async function loadAdmin() {
  const [overview, students, parents, teachers, salaries, salaryPayments, payrollSummary, fees, payments, results, attendance, classes, alerts, notices, admissions, resets] = await Promise.all([
    api('/api/admin/overview'),
    api('/api/admin/students'),
    api('/api/admin/parents'),
    api('/api/admin/teachers'),
    api('/api/admin/salaries'),
    api('/api/admin/salary-payments'),
    api('/api/admin/payroll-summary'),
    api('/api/admin/fees'),
    api('/api/admin/payments'),
    api('/api/admin/results'),
    api('/api/admin/attendance'),
    api('/api/lookups/classes'),
    api('/api/admin/notifications'),
    api('/api/admin/notices'),
    api('/api/admin/admissions'),
    api('/api/admin/reset-requests')
  ]);

  state.adminStudents = students || [];
  state.adminParents = parents || [];
  state.adminTeachers = teachers || [];
  state.adminSalaries = salaries || [];
  state.adminResults = results || [];

  renderStats('adminStats', [
    { label: 'Students', value: overview.students },
    { label: 'Parents', value: overview.parents },
    { label: 'Teachers', value: overview.teachers },
    { label: 'Unpaid Fee', value: currency(overview.unpaid) },
    { label: 'Salary Pending', value: currency(overview.salary_pending || 0) },
    { label: 'Results', value: overview.results }
  ]);

  setTable('studentsTable', ['ID', 'Name', 'Roll', 'Class', 'Section', 'Parent', 'Parent Phone', 'Pending Fee', 'Present', 'Absent', 'Portal ID', 'Address'],
    students.map(s => [s.id, s.name, s.roll_no || '', s.class_name || '', s.section || '', s.parent_name || '', s.parent_phone || '', currency(s.pending_fee || 0), s.attendance_present || 0, s.attendance_absent || 0, s.portal_email || '', s.address || ''])
  );
  setTable('parentsTable', ['ID', 'Name', 'Phone', 'Email', 'Children', 'Total Pending', 'Portal ID', 'Address'],
    parents.map(p => [p.id, p.name, p.phone || '', p.email || '', `${p.children_count || 0} • ${p.children_names || ''}`, currency(p.total_pending_fee || 0), p.portal_email || '', p.address || ''])
  );
  setTable('teachersTable', ['ID', 'Name', 'Subject', 'Assigned Class', 'Phone', 'Email', 'Portal ID', 'Salary Pending'],
    teachers.map(t => [t.id, t.name, t.subject || '', t.assigned_class || '', t.phone || '', t.email || '', t.portal_email || '', currency(t.salary_pending || 0)])
  );
  setTable('salaryTable', ['Salary ID', 'Teacher', 'Title', 'Month', 'Gross', 'Bonus', 'Deduction', 'Net', 'Paid', 'Balance', 'Status', 'Due', 'Slip'],
    salaries.map(s => [s.id, s.teacher_name || '', s.title || '', s.month_label || '', currency(s.gross_amount || 0), currency(s.bonus_amount || 0), currency(s.deduction_amount || 0), currency(s.net_amount || 0), currency(s.amount_paid || 0), currency(s.balance_due || 0), badge(s.status), s.due_date || '', linkHtml(s.slip_url, 'Print')])
  );
  setTable('salaryPaymentsTable', ['Payment ID', 'Salary ID', 'Teacher', 'Title', 'Month', 'Amount', 'Type', 'Before', 'Remaining', 'Method', 'Slip'],
    salaryPayments.map(p => [p.id, p.salary_id, p.teacher_name || '', p.salary_title || '', p.month_label || '', currency(p.amount || 0), p.payment_type || '', currency(p.previous_balance || 0), currency(p.remaining_balance || 0), p.method || '', linkHtml(p.slip_url, 'Open')])
  );
  if ($('payrollSummaryTable')) setTable('payrollSummaryTable', ['Month', 'Teachers', 'Gross', 'Bonus', 'Deduction', 'Net', 'Paid', 'Balance', 'Sheet'],
    (payrollSummary.months || []).map(m => [m.month_label || '', m.teachers || 0, currency(m.gross_amount || 0), currency(m.bonus_amount || 0), currency(m.deduction_amount || 0), currency(m.net_amount || 0), currency(m.paid_amount || 0), currency(m.balance_due || 0), linkHtml(m.payroll_sheet_url, 'Print')])
  );
  if ($('teacherYearlyPayrollTable')) setTable('teacherYearlyPayrollTable', ['Teacher', 'Year', 'Months', 'Gross', 'Bonus', 'Deduction', 'Net', 'Paid', 'Balance'],
    (payrollSummary.teachers || []).map(t => [t.teacher_name || '', t.year || '', t.months || 0, currency(t.gross_amount || 0), currency(t.bonus_amount || 0), currency(t.deduction_amount || 0), currency(t.net_amount || 0), currency(t.paid_amount || 0), currency(t.balance_due || 0)])
  );
  setTable('feesTable', ['Fee ID', 'Student', 'Class', 'Title', 'Month', 'Total', 'Paid', 'Balance', 'Status', 'Due', 'Challan'],
    fees.map(f => [
      f.id,
      f.student_name,
      `${f.class_name || ''} ${f.section || ''}`,
      f.title,
      f.month_label || '',
      currency(f.original_amount || 0),
      currency(f.amount_paid || 0),
      currency(f.balance_due || 0),
      badge(f.status),
      f.due_date || '',
      linkHtml(f.challan_url, 'Print')
    ])
  );
  setTable('paymentsTable', ['Payment ID', 'Fee ID', 'Student', 'Title', 'Amount', 'Type', 'Before', 'Remaining', 'Status', 'Method', 'Receipt'],
    payments.map(p => [p.id, p.fee_id, p.student_name, p.fee_title, currency(p.amount), p.payment_type || '', currency(p.previous_balance || 0), currency(p.remaining_balance || 0), badge(p.status), p.method || '', linkHtml(p.receipt_url, 'Open')])
  );
  setTable('attendanceTable', ['ID', 'Date', 'Student', 'Class', 'Status', 'Note'],
    attendance.map(a => [a.id, a.attendance_date, a.student_name, `${a.class_name || ''} ${a.section || ''}`, badge(a.status), a.note || ''])
  );
  if ($('attendanceSummaryTable')) setTable('attendanceSummaryTable', ['Student', 'Class', 'Present', 'Absent', 'Late', 'Total'], buildAttendanceSummaryRows(attendance));
  setTable('resultsTable', ['ID', 'Student', 'Exam', 'Subject', 'Total', 'Obtained', 'Grade', 'Remarks', 'Overall Result', 'Position', 'Result Card', 'Edit'],
    results.map(r => [r.id, r.student_name, r.exam_title, r.subject, r.total_marks, r.obtained_marks, r.grade || '', r.remarks || '', r.result_status || '', r.position_label || '', linkHtml(r.result_card_url, 'Open'), `<button type="button" class="btn secondary btn-xs" onclick='window.editResultCard(${Number(r.student_id)}, ${JSON.stringify(r.exam_title || '')})'>Edit</button>`])
  );
  setTable('alertsTable', ['ID', 'Target', 'Phone', 'Channel', 'Message', 'WhatsApp'],
    alerts.map(a => [a.id, a.target_name || '', a.phone || '', a.channel, a.message, linkHtml(a.wa_link, 'Send')])
  );
  setTable('admissionsTable', ['ID', 'Student', 'Father', 'Class', 'Phone', 'Address', 'Status', 'Slip', 'Date'],
    admissions.map(a => [a.id, a.student_name || '', a.father_name || '', a.class_name || '', a.phone || '', a.address || '', badge(a.status), linkHtml(a.slip_url, 'Print'), String(a.created_at || '').slice(0, 10)])
  );
  setTable('noticesTable', ['ID', 'Title', 'Audience', 'Notice', 'Date'],
    notices.map(n => [n.id, n.title || '', n.audience || 'all', n.body || '', String(n.created_at || '').slice(0, 10)])
  );
  setTable('adminResetRequestsTable', ['ID', 'Email', 'Role', 'Status', 'Date', 'Note'],
    resets.map(r => [r.id, r.email || '', r.role_hint || '', badge(r.status), String(r.created_at || '').slice(0, 10), r.note || ''])
  );

  fillSelect('studentParentSelect', [`<option value="">No Parent Link</option>`, ...parents.map(p => `<option value="${p.id}">${p.name}</option>`)]);
  const studentOptions = students.map(s => `<option value="${s.id}">${s.name} - ${s.class_name || ''}${s.section ? ` (${s.section})` : ''}</option>`);
  const teacherOptions = teachers.map(t => `<option value="${t.id}">${t.name}${t.subject ? ` - ${t.subject}` : ''}</option>`);
  fillSelect('feeStudentSelect', studentOptions);
  fillSelect('attendanceStudentSelect', studentOptions);
  fillSelect('resultStudentSelect', studentOptions, 'Select a student');
  fillSelect('studentEditSelect', studentOptions, 'Select a student');
  fillSelect('studentEditParentSelect', [`<option value="">No Parent Link</option>`, ...parents.map(p => `<option value="${p.id}">${p.name}</option>`)]);
  fillSelect('classSelect', [`<option value="all">All Students</option>`, ...classes.map(c => `<option value="${c}">${c}</option>`)]);
  fillSelect('salaryTeacherSelect', teacherOptions, 'Select a teacher');
  if (students[0]) {
    updateStudentEditForm(students[0].id);
    updateResultStudentPreview(students[0].id);
  }

  if (overview.school) {
    populateSchoolBrandingPreview(overview.school);
    const form = $('schoolSettingsForm');
    if (form) {
      form.name.value = overview.school.name || '';
      form.tagline.value = overview.school.tagline || '';
      form.phone.value = overview.school.phone || '';
      form.address.value = overview.school.address || '';
      form.logoUrl.value = overview.school.logo_url || '';
      form.primaryColor.value = overview.school.primary_color || '#2146d0';
      form.whatsappNumber.value = overview.school.whatsapp_number || '';
    }
  }
}

async function loadTeacher() {
  const [profile, overview, students, attendance, results, notices, salaries, salaryPayments, yearlyReport] = await Promise.all([
    api('/api/teacher/profile'),
    api('/api/teacher/overview'),
    api('/api/teacher/students'),
    api('/api/teacher/attendance'),
    api('/api/teacher/results'),
    api('/api/teacher/notices'),
    api('/api/teacher/salaries'),
    api('/api/teacher/salary-payments'),
    api('/api/teacher/yearly-salary-report')
  ]);

  state.teacherSalaries = salaries || [];

  if ($('teacherProfile')) $('teacherProfile').innerHTML = `
    <div><strong>Name:</strong> ${profile.name || ''}</div>
    <div><strong>Subject:</strong> ${profile.subject || ''}</div>
    <div><strong>Assigned Class:</strong> ${profile.assigned_class || ''}</div>
    <div><strong>Phone:</strong> ${profile.phone || ''}</div>
    <div><strong>Email:</strong> ${profile.email || ''}</div>
    <div><strong>Assigned Students:</strong> ${profile.assigned_students || 0}</div>
    <div><strong>Total Salary:</strong> ${currency(profile.salary_total || 0)}</div>
    <div><strong>Paid Salary:</strong> ${currency(profile.salary_paid || 0)}</div>
    <div><strong>Pending Salary:</strong> ${currency(profile.salary_pending || 0)}</div>
    <div><strong>School:</strong> ${profile.school?.name || ''}</div>
  `;
  renderStats('teacherStats', [
    { label: 'Students', value: overview.students },
    { label: 'Attendance', value: overview.attendance },
    { label: 'Results', value: overview.results },
    { label: 'Salary Pending', value: currency(overview.salary_pending || 0) }
  ]);

  const studentOptions = students.map(s => `<option value="${s.id}">${s.name} - ${s.class_name || ''}${s.section ? ` (${s.section})` : ''}</option>`);
  fillSelect('teacherAttendanceStudentSelect', studentOptions);
  fillSelect('teacherResultStudentSelect', studentOptions);

  if ($('teacherAttendanceSummaryTable')) setTable('teacherAttendanceSummaryTable', ['Student', 'Class', 'Present', 'Absent', 'Late', 'Total'], buildAttendanceSummaryRows(attendance));
  setTable('teacherAttendanceTable', ['ID', 'Date', 'Student', 'Class', 'Status', 'Note'],
    attendance.map(a => [a.id, a.attendance_date, a.student_name, `${a.class_name || ''} ${a.section || ''}`, badge(a.status), a.note || ''])
  );
  setTable('teacherResultsTable', ['ID', 'Student', 'Exam', 'Subject', 'Total', 'Obtained', 'Grade', 'Result Card'],
    results.map(r => [r.id, r.student_name, r.exam_title, r.subject, r.total_marks, r.obtained_marks, r.grade || '', linkHtml(r.result_card_url, 'Open')])
  );
  setTable('teacherSalaryTable', ['Salary ID', 'Title', 'Month', 'Gross', 'Bonus', 'Deduction', 'Net', 'Paid', 'Balance', 'Status', 'Due', 'Slip'],
    salaries.map(s => [s.id, s.title || '', s.month_label || '', currency(s.gross_amount || 0), currency(s.bonus_amount || 0), currency(s.deduction_amount || 0), currency(s.net_amount || 0), currency(s.amount_paid || 0), currency(s.balance_due || 0), badge(s.status), s.due_date || '', linkHtml(s.slip_url, 'Print')])
  );
  setTable('teacherSalaryPaymentsTable', ['Payment ID', 'Salary ID', 'Title', 'Month', 'Amount', 'Type', 'Before', 'Remaining', 'Method'],
    salaryPayments.map(p => [p.id, p.salary_id, p.salary_title || '', p.month_label || '', currency(p.amount || 0), p.payment_type || '', currency(p.previous_balance || 0), currency(p.remaining_balance || 0), p.method || ''])
  );
  if ($('teacherYearlySalaryTable')) setTable('teacherYearlySalaryTable', ['Year', 'Months', 'Gross', 'Bonus', 'Deduction', 'Net', 'Paid', 'Balance'],
    (yearlyReport || []).map(y => [y.year || '', y.months || 0, currency(y.gross_amount || 0), currency(y.bonus_amount || 0), currency(y.deduction_amount || 0), currency(y.net_amount || 0), currency(y.paid_amount || 0), currency(y.balance_due || 0)])
  );
  setTable('teacherNoticesTable', ['ID', 'Title', 'Audience', 'Notice', 'Date'],
    notices.map(n => [n.id, n.title || '', n.audience || 'all', n.body || '', String(n.created_at || '').slice(0, 10)])
  );
}

async function loadParent() {
  const [profile, children, fees, results, payments, notices] = await Promise.all([
    api('/api/parent/profile'),
    api('/api/parent/children'),
    api('/api/parent/fees'),
    api('/api/parent/results'),
    api('/api/parent/payments'),
    api('/api/parent/notices')
  ]);
  if ($('parentProfile')) $('parentProfile').innerHTML = `
    <div><strong>Name:</strong> ${profile.name || ''}</div>
    <div><strong>Phone:</strong> ${profile.phone || ''}</div>
    <div><strong>Email:</strong> ${profile.email || ''}</div>
    <div><strong>Address:</strong> ${profile.address || ''}</div>
    <div><strong>Total Children:</strong> ${profile.children_count || 0}</div>
    <div><strong>Total Pending Fee:</strong> ${currency(profile.total_pending_fee || 0)}</div>
    <div><strong>Total Paid:</strong> ${currency(profile.total_paid || 0)}</div>
  `;
  setTable('childrenTable', ['ID', 'Name', 'Class', 'Section', 'Roll', 'Pending Fee', 'Results', 'Present', 'Absent', 'Address'],
    children.map(c => [c.id, c.name, c.class_name || '', c.section || '', c.roll_no || '', currency(c.pending_fee || 0), c.result_count || 0, c.attendance_present || 0, c.attendance_absent || 0, c.address || ''])
  );
  setTable('parentFeesTable', ['Fee ID', 'Student', 'Title', 'Month', 'Total', 'Balance', 'Status', 'Due', 'Challan', 'WhatsApp', 'SMS'],
    fees.map(f => [f.id, f.student_name, f.title, f.month_label || '', currency(f.original_amount || 0), currency(f.balance_due || 0), badge(f.status), f.due_date || '', linkHtml(f.challan_url, 'Print'), linkHtml(f.wa_link, 'Send'), linkHtml(f.sms_link, 'Open')])
  );
  setTable('parentPaymentsTable', ['Payment ID', 'Fee ID', 'Student', 'Amount', 'Status', 'Method', 'Ref', 'Receipt'],
    payments.map(p => [p.id, p.fee_id, p.student_name, currency(p.amount), badge(p.status), p.method || '', p.reference_no || '', linkHtml(p.receipt_url, 'Open')])
  );
  setTable('parentResultsTable', ['ID', 'Student', 'Exam', 'Subject', 'Total', 'Obtained', 'Grade', 'Result Card'],
    results.map(r => [r.id, r.student_name, r.exam_title, r.subject, r.total_marks, r.obtained_marks, r.grade || '', linkHtml(r.result_card_url, 'Open')])
  );
  setTable('parentNoticesTable', ['ID', 'Title', 'Audience', 'Notice', 'Date'],
    notices.map(n => [n.id, n.title || '', n.audience || 'all', n.body || '', String(n.created_at || '').slice(0, 10)])
  );
}

async function loadStudent() {
  const [profile, fees, results, notices] = await Promise.all([
    api('/api/student/profile'),
    api('/api/student/fees'),
    api('/api/student/results'),
    api('/api/student/notices')
  ]);
  $('studentProfile').innerHTML = `
    <div><strong>Name:</strong> ${profile.name || ''}</div>
    <div><strong>Roll No:</strong> ${profile.roll_no || ''}</div>
    <div><strong>Class:</strong> ${profile.class_name || ''}</div>
    <div><strong>Section:</strong> ${profile.section || ''}</div>
    <div><strong>Parent:</strong> ${profile.parent_name || ''}</div>
    <div><strong>Parent Phone:</strong> ${profile.parent_phone || ''}</div>
    <div><strong>Parent Email:</strong> ${profile.parent_email || ''}</div>
    <div><strong>Phone:</strong> ${profile.phone || ''}</div>
    <div><strong>Address:</strong> ${profile.address || ''}</div>
    <div><strong>Total Fee:</strong> ${currency(profile.total_fee || 0)}</div>
    <div><strong>Total Paid:</strong> ${currency(profile.total_paid || 0)}</div>
    <div><strong>Pending Fee:</strong> ${currency(profile.pending_fee || 0)}</div>
    <div><strong>Attendance:</strong> Present ${profile.attendance_present || 0} / Absent ${profile.attendance_absent || 0}</div>
  `;
  setTable('studentFeesTable', ['Fee ID', 'Title', 'Month', 'Total', 'Balance', 'Status', 'Due', 'Challan', 'WhatsApp', 'SMS'],
    fees.map(f => [f.id, f.title, f.month_label || '', currency(f.original_amount || 0), currency(f.balance_due || 0), badge(f.status), f.due_date || '', linkHtml(f.challan_url, 'Print'), linkHtml(f.wa_link, 'Send'), linkHtml(f.sms_link, 'Open')])
  );
  setTable('studentResultsTable', ['ID', 'Exam', 'Subject', 'Total', 'Obtained', 'Grade', 'Remarks', 'Result Card'],
    results.map(r => [r.id, r.exam_title, r.subject, r.total_marks, r.obtained_marks, r.grade || '', r.remarks || '', linkHtml(r.result_card_url, 'Open')])
  );
  setTable('studentNoticesTable', ['ID', 'Title', 'Audience', 'Notice', 'Date'],
    notices.map(n => [n.id, n.title || '', n.audience || 'all', n.body || '', String(n.created_at || '').slice(0, 10)])
  );
}

async function hydrate() {
  setViewState();
  if (!state.token || !state.user) return;
  try {
    if (state.user.role === 'owner') await loadOwner();
    if (state.user.role === 'admin') await loadAdmin();
    if (state.user.role === 'teacher') await loadTeacher();
    if (state.user.role === 'parent') await loadParent();
    if (state.user.role === 'student') await loadStudent();
  } catch (error) {
    showMessage('loginMessage', error.message, true);
  }
}

$('toggleAdmissionBtn')?.addEventListener('click', () => {
  const wrap = $('admissionFormWrap');
  if (!wrap) return;
  wrap.classList.toggle('hidden');
  $('toggleAdmissionBtn').textContent = wrap.classList.contains('hidden') ? 'Open Admission Form' : 'Hide Admission Form';
});

$('toggleResetBtn')?.addEventListener('click', () => {
  $('resetCard').classList.toggle('hidden');
});


$('studentEditSelect')?.addEventListener('change', (e) => {
  updateStudentEditForm(e.target.value);
});

$('resultStudentSelect')?.addEventListener('change', (e) => {
  const form = $('resultForm');
  if (form && form.originalExamTitle) form.originalExamTitle.value = '';
  setResultEditState('', '');
  updateResultStudentPreview(e.target.value);
});

$('clearResultEditBtn')?.addEventListener('click', () => {
  clearResultForm(true);
});

$('logoutBtn')?.addEventListener('click', () => {
  clearSession();
  setViewState();
});

$('setupForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/setup/initialize', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('setupMessage', data.message);
    state.initialized = true;
    form.reset();
    setViewState();
  } catch (error) {
    showMessage('setupMessage', error.message, true);
  }
});

$('loadDemoBtn')?.addEventListener('click', async () => {
  try {
    const data = await api('/api/setup/load-demo', { method: 'POST', body: JSON.stringify({}) });
    showMessage('setupMessage', data.message);
    state.initialized = true;
    setViewState();
  } catch (error) {
    showMessage('setupMessage', error.message, true);
  }
});

$('loginForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  showMessage('loginMessage', '');
  const body = formDataObject(form);
  if (!body.schoolCode) delete body.schoolCode;
  try {
    const data = await api('/api/auth/login', { method: 'POST', body: JSON.stringify(body) });
    saveSession(data.token, data.user);
    form.reset();
    await hydrate();
  } catch (error) {
    showMessage('loginMessage', error.message, true);
  }
});

$('onboardingForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/public/onboarding-request', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('onboardingMessage', data.message);
    form.reset();
  } catch (error) {
    showMessage('onboardingMessage', error.message, true);
  }
});

$('admissionForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/public/admissions', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('admissionMessage', data.message);
    form.reset();
  } catch (error) {
    showMessage('admissionMessage', error.message, true);
  }
});

$('resetForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/auth/request-reset', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('resetMessage', data.message);
    form.reset();
  } catch (error) {
    showMessage('resetMessage', error.message, true);
  }
});

$('createSchoolForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/owner/schools', { method: 'POST', body: JSON.stringify(await collectFormDataWithLogo(form)) });
    showMessage('ownerActionMessage', data.message);
    form.reset();
    await loadOwner();
  } catch (error) {
    showMessage('ownerActionMessage', error.message, true);
  }
});

$('brandingForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/owner/schools/branding', { method: 'PATCH', body: JSON.stringify(await collectFormDataWithLogo(form)) });
    showMessage('brandingMessage', data.message);
    await loadOwner();
  } catch (error) {
    showMessage('brandingMessage', error.message, true);
  }
});

$('ownerSchoolSelect')?.addEventListener('change', (e) => {
  const school = state.ownerSchools.find(s => s.id === Number(e.target.value));
  const form = $('brandingForm');
  if (!school || !form) return;
  form.name.value = school.name || '';
  form.tagline.value = school.tagline || '';
  form.phone.value = school.phone || '';
  form.address.value = school.address || '';
  form.logoUrl.value = school.logo_url || '';
  form.primaryColor.value = school.primary_color || '#2146d0';
  form.whatsappNumber.value = school.whatsapp_number || '';
});

$('ownerResetHandleForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/owner/reset-password', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('ownerResetMessage', data.message);
    form.reset();
    await loadOwner();
  } catch (error) {
    showMessage('ownerResetMessage', error.message, true);
  }
});

$('parentForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/parents', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('parentMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('parentMessage', error.message, true);
  }
});

$('studentForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  if (!body.parentId) delete body.parentId;
  try {
    const data = await api('/api/admin/students', { method: 'POST', body: JSON.stringify(body) });
    showMessage('studentMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('studentMessage', error.message, true);
  }
});

$('studentEditForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  const studentId = body.studentId;
  delete body.studentId;
  try {
    const data = await api(`/api/admin/students/${studentId}`, { method: 'PATCH', body: JSON.stringify(body) });
    showMessage('studentEditMessage', data.message);
    await loadAdmin();
  } catch (error) {
    showMessage('studentEditMessage', error.message, true);
  }
});

$('teacherForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/teachers', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('teacherMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('teacherMessage', error.message, true);
  }
});

$('assignSalaryForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/salaries/assign', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('salaryMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('salaryMessage', error.message, true);
  }
});

$('salaryPaymentForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  const salaryId = body.salaryId;
  delete body.salaryId;
  try {
    const data = await api(`/api/admin/salaries/${salaryId}/mark-paid`, { method: 'POST', body: JSON.stringify(body) });
    showMessage('salaryPaymentMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('salaryPaymentMessage', error.message, true);
  }
});

$('assignFeeForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/fees/assign', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('feeMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('feeMessage', error.message, true);
  }
});

$('bulkFeeForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/fees/bulk-assign', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('bulkFeeMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('bulkFeeMessage', error.message, true);
  }
});

$('verifyFeeForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  const feeId = body.feeId;
  delete body.feeId;
  try {
    const data = await api(`/api/admin/fees/${feeId}/mark-paid`, { method: 'POST', body: JSON.stringify(body) });
    showMessage('verifyFeeMessage', data.message + (data.receipt_url ? ` Receipt: ${data.receipt_url}` : ''));
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('verifyFeeMessage', error.message, true);
  }
});

$('attendanceForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/attendance/mark', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('attendanceMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('attendanceMessage', error.message, true);
  }
});

$('resultForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const body = formDataObject(form);
    const selectedStudent = body.studentId;
    const data = await api('/api/admin/results/batch', { method: 'POST', body: JSON.stringify(body) });
    showMessage('resultMessage', data.message);
    await loadAdmin();
    clearResultForm(true);
    if ($('resultStudentSelect')) $('resultStudentSelect').value = selectedStudent;
    updateResultStudentPreview(selectedStudent);
  } catch (error) {
    showMessage('resultMessage', error.message, true);
  }
});

$('schoolSettingsForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/school-settings', { method: 'PATCH', body: JSON.stringify(await collectFormDataWithLogo(form)) });
    showMessage('schoolSettingsMessage', data.message);
    await loadAdmin();
  } catch (error) {
    showMessage('schoolSettingsMessage', error.message, true);
  }
});

$('noticeForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/notices', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('noticeMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('noticeMessage', error.message, true);
  }
});

$('sheetSyncForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/sync/students-from-sheet', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('sheetSyncMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('sheetSyncMessage', error.message, true);
  }
});

$('generateAlertsBtn')?.addEventListener('click', async () => {
  try {
    const data = await api('/api/admin/notifications/generate-fee-alerts', { method: 'POST', body: JSON.stringify({}) });
    showMessage('alertsMessage', data.message);
    await loadAdmin();
  } catch (error) {
    showMessage('alertsMessage', error.message, true);
  }
});


$('adminResetHandleForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/admin/reset-password', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('adminResetMessage', data.message);
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('adminResetMessage', error.message, true);
  }
});

$('admissionApproveForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  const requestId = body.requestId;
  delete body.requestId;
  try {
    const data = await api(`/api/admin/admissions/${requestId}/approve`, { method: 'POST', body: JSON.stringify(body) });
    showMessage('admissionApproveMessage', data.message + (data.slip_url ? ` Slip: ${data.slip_url}` : ''));
    form.reset();
    await loadAdmin();
  } catch (error) {
    showMessage('admissionApproveMessage', error.message, true);
  }
});

$('parentSubmitFeeForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const body = formDataObject(form);
  const feeId = body.feeId;
  delete body.feeId;
  try {
    const data = await api(`/api/parent/fees/${feeId}/submit-payment`, { method: 'POST', body: JSON.stringify(body) });
    showMessage('parentFeeMessage', data.message);
    form.reset();
    await loadParent();
  } catch (error) {
    showMessage('parentFeeMessage', error.message, true);
  }
});

$('teacherAttendanceForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/teacher/attendance/mark', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('teacherAttendanceMessage', data.message);
    form.reset();
    await loadTeacher();
  } catch (error) {
    showMessage('teacherAttendanceMessage', error.message, true);
  }
});

$('teacherResultForm')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  try {
    const data = await api('/api/teacher/results', { method: 'POST', body: JSON.stringify(formDataObject(form)) });
    showMessage('teacherResultMessage', data.message);
    form.reset();
    await loadTeacher();
  } catch (error) {
    showMessage('teacherResultMessage', error.message, true);
  }
});

document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    const panel = document.getElementById(btn.dataset.tab);
    if (panel) panel.classList.add('active');
  });
});

boot();
