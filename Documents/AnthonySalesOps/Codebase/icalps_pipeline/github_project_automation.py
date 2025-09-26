#!/usr/bin/env python3
"""
GitHub Project Automation for IC'ALPS Pipeline
Sets up automation rules, workflows, and project board management
"""

import subprocess
import json
import sys
from typing import Dict, List, Any

class GitHubProjectAutomation:
    def __init__(self, repo="Wkayaobama/icalps_pipeline"):
        self.repo = repo
        self.project_id = None

    def run_gh_command(self, command_list):
        """Run GitHub CLI command and return output"""
        try:
            result = subprocess.run(
                command_list,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {' '.join(command_list)}")
            print(f"Error: {e.stderr}")
            return None

    def find_project_id(self):
        """Find the project ID for our IC-ALPS Pipeline project"""
        cmd = ['gh', 'project', 'list', '--owner', 'Wkayaobama', '--format', 'json']
        result = self.run_gh_command(cmd)

        if result:
            projects = json.loads(result)
            for project in projects['projects']:
                if 'IC-ALPS Pipeline' in project['title']:
                    self.project_id = project['number']
                    print(f"Found project ID: {self.project_id}")
                    return self.project_id

        print("Project not found. Please create it first with setup_github_project.py")
        return None

    def setup_project_columns(self):
        """Set up project board columns"""
        print("\\n=== SETTING UP PROJECT COLUMNS ===")

        if not self.project_id:
            if not self.find_project_id():
                return False

        # Standard columns for Kanban workflow
        columns = [
            {"name": "📋 Backlog", "description": "Issues ready to be worked on"},
            {"name": "🏗️ Ready", "description": "Issues prepared for development"},
            {"name": "⚡ In Progress", "description": "Currently active development"},
            {"name": "👀 Review", "description": "Code review and testing phase"},
            {"name": "✅ Done", "description": "Completed and delivered"}
        ]

        for column in columns:
            print(f"Column to create: {column['name']}")
            # Note: GitHub Projects V2 uses different API structure
            # This would require GraphQL API calls for full automation

        return True

    def create_automation_workflows(self):
        """Create GitHub Actions workflows for automation"""
        print("\\n=== CREATING AUTOMATION WORKFLOWS ===")

        # Issue Auto-labeler workflow
        issue_labeler = """name: Auto Label Issues

on:
  issues:
    types: [opened, edited]

jobs:
  label:
    runs-on: ubuntu-latest
    steps:
      - name: Label by Title Prefix
        uses: actions/github-script@v6
        with:
          script: |
            const title = context.payload.issue.title;
            const labels = [];

            // Add labels based on title prefix
            if (title.includes('[INFRA]')) {
              labels.push('infrastructure');
            }
            if (title.includes('[FEATURE]')) {
              labels.push('feature');
            }
            if (title.includes('[DOCS]')) {
              labels.push('documentation');
            }
            if (title.includes('[BUG]')) {
              labels.push('bug');
            }

            // Add phase labels
            if (title.includes('Database') || title.includes('Connection')) {
              labels.push('database');
            }
            if (title.includes('xlwings') || title.includes('Excel')) {
              labels.push('xlwings');
            }
            if (title.includes('DuckDB') || title.includes('Processing')) {
              labels.push('duckdb');
            }

            if (labels.length > 0) {
              await github.rest.issues.addLabels({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: context.payload.issue.number,
                labels: labels
              });
            }
"""

        # Project board automation workflow
        project_automation = """name: Project Board Automation

on:
  issues:
    types: [opened, assigned, closed]
  pull_request:
    types: [opened, closed, merged]

jobs:
  update-project:
    runs-on: ubuntu-latest
    steps:
      - name: Update Project Board
        uses: actions/github-script@v6
        with:
          script: |
            const issue = context.payload.issue || context.payload.pull_request;
            const action = context.payload.action;

            // Log the action for debugging
            console.log(`Action: ${action}, Issue: ${issue.number}`);

            // Auto-assign issues to project
            if (action === 'opened' && context.payload.issue) {
              console.log('Issue opened, would add to project board');
              // Project V2 API integration would go here
            }

            // Move to "In Progress" when assigned
            if (action === 'assigned' && context.payload.issue) {
              console.log('Issue assigned, would move to In Progress');
              // Project V2 API integration would go here
            }

            // Move to "Done" when closed
            if (action === 'closed' && context.payload.issue) {
              console.log('Issue closed, would move to Done');
              // Project V2 API integration would go here
            }
"""

        # Milestone progress tracking
        milestone_tracker = """name: Milestone Progress Tracker

on:
  issues:
    types: [closed, reopened]
  schedule:
    - cron: '0 9 * * 1'  # Weekly on Monday

jobs:
  track-progress:
    runs-on: ubuntu-latest
    steps:
      - name: Generate Milestone Report
        uses: actions/github-script@v6
        with:
          script: |
            // Get all milestones
            const milestones = await github.rest.issues.listMilestones({
              owner: context.repo.owner,
              repo: context.repo.repo,
              state: 'open'
            });

            for (const milestone of milestones.data) {
              const issues = await github.rest.issues.listForRepo({
                owner: context.repo.owner,
                repo: context.repo.repo,
                milestone: milestone.number,
                state: 'all'
              });

              const total = issues.data.length;
              const closed = issues.data.filter(issue => issue.state === 'closed').length;
              const progress = total > 0 ? Math.round((closed / total) * 100) : 0;

              console.log(`Milestone: ${milestone.title}`);
              console.log(`Progress: ${closed}/${total} (${progress}%)`);

              // Create progress comment or update milestone description
              if (progress === 100) {
                console.log(`🎉 Milestone ${milestone.title} completed!`);
              }
            }
"""

        workflows = {
            "issue-labeler.yml": issue_labeler,
            "project-automation.yml": project_automation,
            "milestone-tracker.yml": milestone_tracker
        }

        for filename, content in workflows.items():
            with open(f".github/workflows/{filename}", 'w') as f:
                f.write(content)
            print(f"✓ Created workflow: {filename}")

        return True

    def setup_branch_protection(self):
        """Set up branch protection rules"""
        print("\\n=== SETTING UP BRANCH PROTECTION ===")

        protection_rules = {
            "required_status_checks": {
                "strict": True,
                "contexts": ["ci/test", "ci/lint"]
            },
            "enforce_admins": False,
            "required_pull_request_reviews": {
                "required_approving_review_count": 1,
                "dismiss_stale_reviews": True
            },
            "restrictions": None,
            "allow_force_pushes": False,
            "allow_deletions": False
        }

        cmd = [
            'gh', 'api', f'repos/{self.repo}/branches/main/protection',
            '-X', 'PUT',
            '--input', '-'
        ]

        try:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=json.dumps(protection_rules))

            if process.returncode == 0:
                print("✓ Branch protection rules configured")
                return True
            else:
                print(f"✗ Failed to set branch protection: {stderr}")
                return False

        except Exception as e:
            print(f"Error setting up branch protection: {e}")
            return False

    def create_issue_templates_config(self):
        """Create issue template configuration"""
        print("\\n=== CREATING ISSUE TEMPLATE CONFIG ===")

        template_config = {
            "blank_issues_enabled": False,
            "contact_links": [
                {
                    "name": "📊 Project Board",
                    "url": f"https://github.com/{self.repo}/projects",
                    "about": "View project progress and manage tasks"
                },
                {
                    "name": "📚 Documentation",
                    "url": f"https://github.com/{self.repo}/blob/main/PROJECT_MANAGEMENT.md",
                    "about": "Project management documentation and workflows"
                }
            ]
        }

        with open(".github/ISSUE_TEMPLATE/config.yml", 'w') as f:
            import yaml
            yaml.dump(template_config, f, default_flow_style=False)

        print("✓ Issue template configuration created")
        return True

    def generate_project_status_script(self):
        """Generate a script for project status reporting"""
        print("\\n=== CREATING PROJECT STATUS SCRIPT ===")

        status_script = """#!/usr/bin/env python3
\"\"\"
Project Status Reporter for IC'ALPS Pipeline
Generates comprehensive project status reports
\"\"\"

import json
import subprocess
from datetime import datetime, timedelta

class ProjectStatusReporter:
    def __init__(self, repo="Wkayaobama/icalps_pipeline"):
        self.repo = repo

    def run_gh_command(self, command_list):
        try:
            result = subprocess.run(
                command_list,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error: {e.stderr}")
            return None

    def get_milestone_progress(self):
        \"\"\"Get progress for all milestones\"\"\"
        cmd = ['gh', 'api', f'repos/{self.repo}/milestones']
        result = self.run_gh_command(cmd)

        if not result:
            return []

        milestones = json.loads(result)
        progress_data = []

        for milestone in milestones:
            total_issues = milestone['open_issues'] + milestone['closed_issues']
            if total_issues > 0:
                completion_rate = (milestone['closed_issues'] / total_issues) * 100
            else:
                completion_rate = 0

            progress_data.append({
                'title': milestone['title'],
                'due_date': milestone['due_on'][:10] if milestone['due_on'] else 'No due date',
                'total_issues': total_issues,
                'completed': milestone['closed_issues'],
                'remaining': milestone['open_issues'],
                'completion_rate': round(completion_rate, 1)
            })

        return progress_data

    def get_recent_activity(self, days=7):
        \"\"\"Get recent issue activity\"\"\"
        since_date = (datetime.now() - timedelta(days=days)).isoformat()

        cmd = ['gh', 'api', f'repos/{self.repo}/issues',
               '-f', f'since={since_date}', '-f', 'state=all']
        result = self.run_gh_command(cmd)

        if not result:
            return []

        issues = json.loads(result)
        return [{
            'number': issue['number'],
            'title': issue['title'],
            'state': issue['state'],
            'updated_at': issue['updated_at'][:10],
            'labels': [label['name'] for label in issue['labels']]
        } for issue in issues[:10]]  # Last 10 issues

    def generate_status_report(self):
        \"\"\"Generate comprehensive status report\"\"\"
        print("=== IC'ALPS PIPELINE PROJECT STATUS ===")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print()

        # Milestone Progress
        print("📊 MILESTONE PROGRESS")
        print("-" * 50)
        milestones = self.get_milestone_progress()

        for milestone in milestones:
            status_bar = "█" * int(milestone['completion_rate'] / 5) + "░" * (20 - int(milestone['completion_rate'] / 5))
            print(f"{milestone['title']}")
            print(f"  Due: {milestone['due_date']}")
            print(f"  Progress: [{status_bar}] {milestone['completion_rate']}%")
            print(f"  Issues: {milestone['completed']}/{milestone['total_issues']} completed")
            print()

        # Recent Activity
        print("🔄 RECENT ACTIVITY (Last 7 days)")
        print("-" * 50)
        recent = self.get_recent_activity()

        for activity in recent:
            status_emoji = "✅" if activity['state'] == 'closed' else "🔄"
            labels_str = ", ".join(activity['labels'][:3]) if activity['labels'] else "no labels"
            print(f"{status_emoji} #{activity['number']}: {activity['title'][:60]}")
            print(f"    Updated: {activity['updated_at']} | Labels: {labels_str}")
            print()

        # Summary Stats
        total_issues = sum(m['total_issues'] for m in milestones)
        completed_issues = sum(m['completed'] for m in milestones)
        overall_progress = (completed_issues / total_issues * 100) if total_issues > 0 else 0

        print("📈 PROJECT SUMMARY")
        print("-" * 50)
        print(f"Overall Progress: {overall_progress:.1f}%")
        print(f"Total Issues: {total_issues}")
        print(f"Completed: {completed_issues}")
        print(f"Remaining: {total_issues - completed_issues}")
        print(f"Active Milestones: {len([m for m in milestones if m['remaining'] > 0])}")

if __name__ == "__main__":
    reporter = ProjectStatusReporter()
    reporter.generate_status_report()
"""

        with open("project_status.py", 'w') as f:
            f.write(status_script)

        print("✓ Project status script created")
        return True

    def run_full_automation_setup(self):
        """Run the complete automation setup"""
        print("=== IC'ALPS PIPELINE AUTOMATION SETUP ===")

        success = True

        # Set up project columns
        if not self.setup_project_columns():
            success = False

        # Create automation workflows
        if not self.create_automation_workflows():
            success = False

        # Set up branch protection
        if not self.setup_branch_protection():
            success = False

        # Create issue template config
        if not self.create_issue_templates_config():
            success = False

        # Generate status script
        if not self.generate_project_status_script():
            success = False

        if success:
            print("\\n✅ AUTOMATION SETUP COMPLETE")
            print("\\nNext steps:")
            print("1. Review created workflows in .github/workflows/")
            print("2. Configure project board columns manually (GraphQL API required)")
            print("3. Test automation by creating a test issue")
            print("4. Run 'python project_status.py' for status reports")
        else:
            print("\\n❌ Some automation setup steps failed")
            print("Please review errors above and configure manually if needed")

        return success

if __name__ == "__main__":
    automation = GitHubProjectAutomation()
    automation.run_full_automation_setup()
"""