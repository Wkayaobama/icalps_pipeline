#!/usr/bin/env python3
"""
GitHub Project Setup Script for IC'ALPS Pipeline
Creates issues, milestones, and project board with automation
"""

import json
import subprocess
import sys
from datetime import datetime

class GitHubProjectSetup:
    def __init__(self, repo="Wkayaobama/icalps_pipeline"):
        self.repo = repo
        self.issues_data = None
        self.load_issues_data()

    def load_issues_data(self):
        """Load the generated issues data"""
        try:
            with open('github_issues_export.json', 'r') as f:
                self.issues_data = json.load(f)
            print(f"Loaded {len(self.issues_data['issues'])} issues from export file")
        except FileNotFoundError:
            print("ERROR: github_issues_export.json not found. Run generate_github_issues.py first.")
            sys.exit(1)

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

    def check_gh_auth(self):
        """Check if GitHub CLI is authenticated"""
        result = self.run_gh_command(['gh', 'auth', 'status'])
        if result is None:
            print("Please authenticate with GitHub CLI first: gh auth login")
            return False
        print("✓ GitHub CLI authenticated")
        return True

    def create_milestones(self):
        """Create project milestones"""
        print("\\n=== CREATING MILESTONES ===")
        for milestone in self.issues_data['milestones']:
            cmd = [
                'gh', 'api', f'repos/{self.repo}/milestones',
                '-X', 'POST',
                '-f', f'title={milestone["title"]}',
                '-f', f'description={milestone["description"]}',
                '-f', f'due_on={milestone["due_date"]}T23:59:59Z',
                '-f', f'state={milestone["state"]}'
            ]
            result = self.run_gh_command(cmd)
            if result:
                print(f"✓ Created milestone: {milestone['title']}")
            else:
                print(f"✗ Failed to create milestone: {milestone['title']}")

    def create_labels(self):
        """Create custom labels for the project"""
        print("\\n=== CREATING LABELS ===")
        labels = [
            {"name": "phase-1:-foundation-infrastructure", "color": "0052cc", "description": "Phase 1: Foundation Infrastructure tasks"},
            {"name": "phase-2:-core-data-model-implementation", "color": "1d76db", "description": "Phase 2: Core Data Model Implementation"},
            {"name": "phase-3:-xlwings-integration-layer", "color": "0e8a16", "description": "Phase 3: xlwings Integration Layer"},
            {"name": "phase-4:-advanced-features-&-optimization", "color": "fbca04", "description": "Phase 4: Advanced Features & Optimization"},
            {"name": "phase-5:-production-deployment-&-monitoring", "color": "d93f0b", "description": "Phase 5: Production Deployment & Monitoring"},
            {"name": "critical", "color": "b60205", "description": "Critical priority - foundational dependency"},
            {"name": "high", "color": "d93f0b", "description": "High priority - required for core features"},
            {"name": "medium", "color": "fbca04", "description": "Medium priority - important for workflow"},
            {"name": "low", "color": "0e8a16", "description": "Low priority - future enhancement"},
            {"name": "infrastructure", "color": "5319e7", "description": "Infrastructure and foundation work"},
            {"name": "database", "color": "1d76db", "description": "Database-related tasks"},
            {"name": "xlwings", "color": "0052cc", "description": "xlwings Excel integration"},
            {"name": "data-processing", "color": "0e8a16", "description": "Data processing and transformation"},
            {"name": "analytics", "color": "fbca04", "description": "Analytics and reporting features"},
            {"name": "performance", "color": "d93f0b", "description": "Performance optimization"},
            {"name": "production", "color": "b60205", "description": "Production readiness"}
        ]

        for label in labels:
            cmd = [
                'gh', 'api', f'repos/{self.repo}/labels',
                '-X', 'POST',
                '-f', f'name={label["name"]}',
                '-f', f'color={label["color"]}',
                '-f', f'description={label["description"]}'
            ]
            result = self.run_gh_command(cmd)
            if result:
                print(f"✓ Created label: {label['name']}")

    def create_issues(self):
        """Create all GitHub issues"""
        print("\\n=== CREATING ISSUES ===")
        created_issues = []

        for i, issue in enumerate(self.issues_data['issues'], 1):
            print(f"Creating issue {i}/{len(self.issues_data['issues'])}: {issue['title']}")

            # Prepare the issue body with custom fields
            body_with_metadata = issue['body'] + f"""

---
**Project Metadata:**
- **Criticality:** {issue['custom_fields']['criticality']}
- **Estimated Start:** {issue['custom_fields']['start_date']}
- **Estimated End:** {issue['custom_fields']['end_date']}
- **Estimated Hours:** {issue['custom_fields']['estimated_hours']}

*This issue was auto-generated from the IC'ALPS Pipeline development plan.*"""

            # Create issue via GitHub CLI
            cmd = [
                'gh', 'issue', 'create',
                '--repo', self.repo,
                '--title', issue['title'],
                '--body', body_with_metadata,
                '--label', ','.join(issue['labels']),
                '--assignee', ','.join(issue['assignees'])
            ]

            # Add milestone if it exists
            if 'milestone' in issue and issue['milestone']:
                # Get milestone number
                milestone_title = issue['milestone']
                milestones_result = self.run_gh_command(['gh', 'api', f'repos/{self.repo}/milestones'])
                if milestones_result:
                    milestones = json.loads(milestones_result)
                    for ms in milestones:
                        if ms['title'] == milestone_title:
                            cmd.extend(['--milestone', str(ms['number'])])
                            break

            result = self.run_gh_command(cmd)
            if result:
                print(f"✓ Created: {issue['title']}")
                created_issues.append(result)
            else:
                print(f"✗ Failed: {issue['title']}")

        return created_issues

    def create_project_board(self):
        """Create GitHub Project board"""
        print("\\n=== CREATING PROJECT BOARD ===")

        # Create new project (Projects V2)
        cmd = [
            'gh', 'project', 'create',
            '--owner', 'Wkayaobama',
            '--title', 'IC-ALPS Pipeline Development',
            '--body', 'Comprehensive project management for IC-ALPS data pipeline development with xlwings and DuckDB integration.'
        ]

        result = self.run_gh_command(cmd)
        if result:
            project_id = result.split('/')[-1]  # Extract project ID from URL
            print(f"✓ Created project board: {result}")

            # Add custom fields to project
            self.add_project_fields(project_id)
            return project_id
        else:
            print("✗ Failed to create project board")
            return None

    def add_project_fields(self, project_id):
        """Add custom fields to the project"""
        print("\\n=== ADDING PROJECT CUSTOM FIELDS ===")

        fields = [
            {
                "name": "Start Date",
                "data_type": "DATE",
                "description": "Planned start date for the task"
            },
            {
                "name": "End Date",
                "data_type": "DATE",
                "description": "Target completion date for the task"
            },
            {
                "name": "Criticality",
                "data_type": "SINGLE_SELECT",
                "description": "Task criticality level",
                "options": [
                    {"name": "Critical", "color": "RED"},
                    {"name": "High", "color": "ORANGE"},
                    {"name": "Medium", "color": "YELLOW"},
                    {"name": "Low", "color": "GREEN"}
                ]
            },
            {
                "name": "Estimated Hours",
                "data_type": "NUMBER",
                "description": "Estimated hours to complete"
            },
            {
                "name": "Phase",
                "data_type": "SINGLE_SELECT",
                "description": "Development phase",
                "options": [
                    {"name": "Phase 1: Foundation", "color": "BLUE"},
                    {"name": "Phase 2: Data Model", "color": "PURPLE"},
                    {"name": "Phase 3: xlwings Integration", "color": "GREEN"},
                    {"name": "Phase 4: Advanced Features", "color": "ORANGE"},
                    {"name": "Phase 5: Production", "color": "RED"}
                ]
            }
        ]

        # Note: Adding custom fields requires GraphQL API calls
        # This would need more complex implementation for full automation
        print("Custom fields configuration prepared (manual setup required for full automation)")
        for field in fields:
            print(f"- {field['name']}: {field['data_type']}")

    def generate_setup_commands(self):
        """Generate manual setup commands"""
        print("\\n=== MANUAL SETUP COMMANDS ===")
        print("Run these commands to complete the GitHub project setup:")
        print()
        print("1. Create milestones:")
        for milestone in self.issues_data['milestones']:
            print(f"gh api repos/{self.repo}/milestones -X POST -f title='{milestone['title']}' -f description='{milestone['description']}' -f due_on='{milestone['due_date']}T23:59:59Z'")

        print("\\n2. Create project board:")
        print(f"gh project create --owner Wkayaobama --title 'IC-ALPS Pipeline Development' --body 'Comprehensive project management for IC-ALPS pipeline'")

        print("\\n3. Link repository to project:")
        print(f"gh project link {self.repo}")

    def run_full_setup(self):
        """Run the complete GitHub project setup"""
        print("=== IC'ALPS PIPELINE GITHUB PROJECT SETUP ===")

        if not self.check_gh_auth():
            return False

        print(f"Setting up GitHub project for repository: {self.repo}")
        print(f"Total issues to create: {len(self.issues_data['issues'])}")
        print(f"Total milestones to create: {len(self.issues_data['milestones'])}")

        # Prompt for confirmation
        confirm = input("\\nProceed with creating all issues and milestones? (y/N): ")
        if confirm.lower() != 'y':
            print("Setup cancelled. Use --dry-run to see what would be created.")
            return False

        # Execute setup steps
        self.create_labels()
        self.create_milestones()
        created_issues = self.create_issues()
        project_id = self.create_project_board()

        print("\\n=== SETUP COMPLETE ===")
        print(f"✓ Created {len(created_issues)} issues")
        print(f"✓ Created {len(self.issues_data['milestones'])} milestones")
        if project_id:
            print(f"✓ Created project board: {project_id}")

        print("\\nNext steps:")
        print("1. Visit your GitHub repository to review created issues")
        print("2. Configure project board columns and automation rules")
        print("3. Assign issues to project board")
        print("4. Set up branch protection rules")

        return True

    def dry_run(self):
        """Show what would be created without actually creating"""
        print("=== DRY RUN: IC'ALPS PIPELINE GITHUB PROJECT SETUP ===")
        print(f"Repository: {self.repo}")
        print(f"Issues to create: {len(self.issues_data['issues'])}")
        print(f"Milestones to create: {len(self.issues_data['milestones'])}")

        print("\\n=== MILESTONES ===")
        for milestone in self.issues_data['milestones']:
            print(f"- {milestone['title']} (due: {milestone['due_date']})")

        print("\\n=== ISSUES BY PHASE ===")
        phase_counts = {}
        for issue in self.issues_data['issues']:
            for label in issue['labels']:
                if label.startswith('phase-'):
                    phase = label.replace('phase-', '').replace('-', ' ').title()
                    if phase not in phase_counts:
                        phase_counts[phase] = []
                    phase_counts[phase].append(issue['title'])

        for phase, issues in phase_counts.items():
            print(f"\\n{phase}:")
            for issue in issues:
                print(f"  - {issue}")

if __name__ == "__main__":
    setup = GitHubProjectSetup()

    if len(sys.argv) > 1 and sys.argv[1] == '--dry-run':
        setup.dry_run()
    elif len(sys.argv) > 1 and sys.argv[1] == '--commands':
        setup.generate_setup_commands()
    else:
        setup.run_full_setup()