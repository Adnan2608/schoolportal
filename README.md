# Private School Portal - Professional English UI Build

This is a **multi-school private school management portal** with owner, school admin, teacher, parent, and student logins. Each school keeps its own isolated data.

## Core Features

- Multi-school data isolation
- Owner dashboard for all schools
- School admin dashboard
- Teacher portal
- Parent portal with parent profile and total pending fee summary
- Student portal with expanded child profile and pending fee summary
- Student, parent, and teacher management
- Single fee and bulk fee assignment
- Partial and full payment tracking with the same Fee ID and clear remaining balance history
- Attendance and result management with summary tables for present, absent, and late counts
- Batch annual result entry using subject rows
- Editable student profile by admin
- Professional printable fee challan and receipt
- Watermarked result card printing in a school-style report-card format close to the provided sample
- Public online admission form with button toggle and school dropdown/manual select
- Admission approval workflow with printable admission slip
- Notice board for admin, teachers, parents, and students
- School branding (name, color, logo, tagline, WhatsApp)
- Password reset request workflow with hierarchy (Student/Parent/Teacher → Admin, Admin → Owner)
- School onboarding request form
- Google Sheet public CSV import for students
- WhatsApp/SMS fee reminder templates

## Important Note

This build is a **launch-ready MVP**. You can deploy it on a VPS, local server, Render, or Docker.

For a larger public production setup, the next upgrade should be:

- PostgreSQL or MySQL instead of JSON storage
- Real SMS and WhatsApp API integration
- File upload storage
- HTTPS domain setup
- Daily backups
- Audit logging

## Run Locally

```bash
node server.js
```

Open:

```bash
http://localhost:3000
```

## First Launch

If `data/db.json` is empty or missing, the portal will open in setup mode.

You have two options:

1. **Complete Setup** → create your real owner and school
2. **Load Demo Data** → load ready-made test accounts

## Demo Accounts

- Owner: `owner@portal.local` / `Owner@123`
- Admin: `admin@bfs.local` / `Admin@123` / `BFS001`
- Teacher: `teacher1@bfs.local` / `Teacher@123` / `BFS001`
- Parent: `parent1@bfs.local` / `Parent@123` / `BFS001`
- Student: `student1@bfs.local` / `Student@123` / `BFS001`

## Reset / Fresh Install

To start fresh:

1. Delete `data/db.json`
2. Run `node server.js` again

## Docker

```bash
docker build -t private-school-portal .
docker run -p 3000:3000 private-school-portal
```

## Deploy Suggestion

- Ubuntu VPS
- Node 20
- Nginx reverse proxy
- PM2 or Docker
- Domain + SSL

## Google Sheet CSV Import Format

Headers example:

```text
name,roll_no,class_name,section,parent_name,parent_phone,parent_email,address
Ahmad Ali,101,5,A,Muhammad Aslam,923005551111,aslam@example.com,Street 1
```

Publish the Google Sheet as a public CSV and paste the URL in the admin panel.


## New in this build
- Professional single-page landscape fee challan improved
- Teacher salary module added
- Admin can assign teacher salary
- Bonus and deduction fields added to salary assignment
- Net salary is calculated automatically
- Admin can save full or partial salary payment
- Remaining salary balance updates automatically
- Monthly payroll summary table added
- Teacher yearly salary report added
- Printable teacher salary slip added
- Printable payroll sheet added

### Demo salary data
- Teacher: teacher1@bfs.local / Teacher@123 / BFS001
- March 2026: Gross Rs. 30,000, Bonus Rs. 1,500, Deduction Rs. 1,000, Net Rs. 30,500, Paid in full
- April 2026: Gross Rs. 30,000, Bonus Rs. 2,000, Deduction Rs. 500, Net Rs. 31,500, Paid Rs. 10,000, Remaining Rs. 21,500
