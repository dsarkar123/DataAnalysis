#!/usr/bin/env python3
"""
Query utilities and examples for GitHub MongoDB data

This module provides utility functions and example queries for analyzing
the GitHub data stored in MongoDB.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict

from pymongo import MongoClient
from pymongo.database import Database
from dotenv import load_dotenv

load_dotenv()

class GitHubDataAnalyzer:
    """Analyzer for GitHub data stored in MongoDB"""
    
    def __init__(self, connection_string: str, database_name: str):
        self.client = MongoClient(connection_string)
        self.db: Database = self.client[database_name]
    
    def get_repository_stats(self, owner: str) -> Dict[str, Any]:
        """Get comprehensive repository statistics"""
        repos = list(self.db.repositories.find({'owner.login': owner}))
        
        if not repos:
            return {'error': f'No repositories found for {owner}'}
        
        stats = {
            'total_repositories': len(repos),
            'total_stars': sum(repo.get('stargazers_count', 0) for repo in repos),
            'total_forks': sum(repo.get('forks_count', 0) for repo in repos),
            'total_watchers': sum(repo.get('watchers_count', 0) for repo in repos),
            'languages': defaultdict(int),
            'most_starred': max(repos, key=lambda x: x.get('stargazers_count', 0)),
            'most_forked': max(repos, key=lambda x: x.get('forks_count', 0)),
            'repository_details': []
        }
        
        for repo in repos:
            if repo.get('language'):
                stats['languages'][repo['language']] += 1
            
            # Get additional stats for each repo
            repo_id = repo['id']
            commits_count = self.db.commits.count_documents({'repository_id': repo_id})
            contributors_count = self.db.contributors.count_documents({'repository_id': repo_id})
            prs_count = self.db.pull_requests.count_documents({'repository_id': repo_id})
            issues_count = self.db.issues.count_documents({'repository_id': repo_id})
            
            stats['repository_details'].append({
                'name': repo['name'],
                'stars': repo.get('stargazers_count', 0),
                'forks': repo.get('forks_count', 0),
                'commits': commits_count,
                'contributors': contributors_count,
                'pull_requests': prs_count,
                'issues': issues_count,
                'language': repo.get('language'),
                'created_at': repo.get('created_at'),
                'updated_at': repo.get('updated_at')
            })
        
        return stats
    
    def get_commit_activity(self, owner: str, days: int = 30) -> Dict[str, Any]:
        """Get commit activity over the last N days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get repository IDs for the owner
        repo_ids = [repo['id'] for repo in self.db.repositories.find({'owner.login': owner})]
        
        # Aggregate commit data
        pipeline = [
            {
                '$match': {
                    'repository_id': {'$in': repo_ids},
                    'commit.author.date': {'$gte': cutoff_date.isoformat()}
                }
            },
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': {'$dateFromString': {'dateString': '$commit.author.date'}}}},
                        'author': '$commit.author.name'
                    },
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'_id.date': 1}
            }
        ]
        
        commits = list(self.db.commits.aggregate(pipeline))
        
        # Process data
        daily_commits = defaultdict(int)
        author_commits = defaultdict(int)
        
        for commit in commits:
            date = commit['_id']['date']
            author = commit['_id']['author']
            count = commit['count']
            
            daily_commits[date] += count
            author_commits[author] += count
        
        return {
            'period_days': days,
            'total_commits': sum(daily_commits.values()),
            'daily_commits': dict(daily_commits),
            'top_contributors': dict(sorted(author_commits.items(), key=lambda x: x[1], reverse=True)[:10]),
            'active_days': len(daily_commits)
        }
    
    def get_issue_pr_stats(self, owner: str) -> Dict[str, Any]:
        """Get issue and pull request statistics"""
        repo_ids = [repo['id'] for repo in self.db.repositories.find({'owner.login': owner})]
        
        # Issues stats
        issues_pipeline = [
            {'$match': {'repository_id': {'$in': repo_ids}}},
            {'$group': {
                '_id': '$state',
                'count': {'$sum': 1}
            }}
        ]
        
        # PRs stats
        prs_pipeline = [
            {'$match': {'repository_id': {'$in': repo_ids}}},
            {'$group': {
                '_id': '$state',
                'count': {'$sum': 1}
            }}
        ]
        
        issues_by_state = {item['_id']: item['count'] for item in self.db.issues.aggregate(issues_pipeline)}
        prs_by_state = {item['_id']: item['count'] for item in self.db.pull_requests.aggregate(prs_pipeline)}
        
        return {
            'issues': {
                'total': sum(issues_by_state.values()),
                'by_state': issues_by_state,
                'open_ratio': issues_by_state.get('open', 0) / max(sum(issues_by_state.values()), 1)
            },
            'pull_requests': {
                'total': sum(prs_by_state.values()),
                'by_state': prs_by_state,
                'merged_ratio': prs_by_state.get('merged', 0) / max(sum(prs_by_state.values()), 1)
            }
        }
    
    def get_contributor_analysis(self, owner: str) -> Dict[str, Any]:
        """Analyze contributors across repositories"""
        repo_ids = [repo['id'] for repo in self.db.repositories.find({'owner.login': owner})]
        
        # Get all contributors
        contributors = list(self.db.contributors.find({'repository_id': {'$in': repo_ids}}))
        
        # Analyze contributor data
        contributor_stats = defaultdict(lambda: {'repos': set(), 'total_contributions': 0})
        
        for contributor in contributors:
            login = contributor['login']
            repo_id = contributor['repository_id']
            contributions = contributor.get('contributions', 0)
            
            contributor_stats[login]['repos'].add(repo_id)
            contributor_stats[login]['total_contributions'] += contributions
        
        # Convert sets to counts
        result = []
        for login, stats in contributor_stats.items():
            result.append({
                'login': login,
                'repositories_count': len(stats['repos']),
                'total_contributions': stats['total_contributions']
            })
        
        # Sort by total contributions
        result.sort(key=lambda x: x['total_contributions'], reverse=True)
        
        return {
            'total_contributors': len(result),
            'top_contributors': result[:10],
            'multi_repo_contributors': len([c for c in result if c['repositories_count'] > 1])
        }
    
    def search_repositories(self, query: str, owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search repositories by name or description"""
        filter_query = {
            '$or': [
                {'name': {'$regex': query, '$options': 'i'}},
                {'description': {'$regex': query, '$options': 'i'}}
            ]
        }
        
        if owner:
            filter_query['owner.login'] = owner
        
        repositories = list(self.db.repositories.find(filter_query))
        
        return [{
            'name': repo['name'],
            'full_name': repo['full_name'],
            'description': repo.get('description', ''),
            'language': repo.get('language'),
            'stars': repo.get('stargazers_count', 0),
            'forks': repo.get('forks_count', 0),
            'updated_at': repo.get('updated_at')
        } for repo in repositories]
    
    def get_recent_activity(self, owner: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Get recent activity across all repositories"""
        repo_ids = [repo['id'] for repo in self.db.repositories.find({'owner.login': owner})]
        
        # Recent commits
        recent_commits = list(self.db.commits.find(
            {'repository_id': {'$in': repo_ids}},
            sort=[('commit.author.date', -1)],
            limit=limit
        ))
        
        # Recent issues
        recent_issues = list(self.db.issues.find(
            {'repository_id': {'$in': repo_ids}},
            sort=[('created_at', -1)],
            limit=limit
        ))
        
        # Recent PRs
        recent_prs = list(self.db.pull_requests.find(
            {'repository_id': {'$in': repo_ids}},
            sort=[('created_at', -1)],
            limit=limit
        ))
        
        return {
            'commits': [{
                'sha': commit['sha'][:7],
                'message': commit['commit']['message'].split('\n')[0],
                'author': commit['commit']['author']['name'],
                'date': commit['commit']['author']['date'],
                'repository_id': commit['repository_id']
            } for commit in recent_commits],
            'issues': [{
                'number': issue['number'],
                'title': issue['title'],
                'state': issue['state'],
                'created_at': issue['created_at'],
                'repository_id': issue['repository_id']
            } for issue in recent_issues],
            'pull_requests': [{
                'number': pr['number'],
                'title': pr['title'],
                'state': pr['state'],
                'created_at': pr['created_at'],
                'repository_id': pr['repository_id']
            } for pr in recent_prs]
        }

def example_usage():
    """Example usage of the analyzer"""
    # Configuration
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    database_name = os.getenv('DATABASE_NAME', 'github_data')
    owner = os.getenv('GITHUB_OWNER', 'octocat')
    
    # Initialize analyzer
    analyzer = GitHubDataAnalyzer(mongodb_uri, database_name)
    
    print(f"Analyzing GitHub data for: {owner}")
    print("=" * 50)
    
    # Repository stats
    print("\n1. Repository Statistics:")
    repo_stats = analyzer.get_repository_stats(owner)
    print(f"Total repositories: {repo_stats.get('total_repositories', 0)}")
    print(f"Total stars: {repo_stats.get('total_stars', 0)}")
    print(f"Total forks: {repo_stats.get('total_forks', 0)}")
    
    if 'most_starred' in repo_stats:
        most_starred = repo_stats['most_starred']
        print(f"Most starred repo: {most_starred['name']} ({most_starred.get('stargazers_count', 0)} stars)")
    
    # Commit activity
    print("\n2. Commit Activity (Last 30 days):")
    commit_activity = analyzer.get_commit_activity(owner, 30)
    print(f"Total commits: {commit_activity['total_commits']}")
    print(f"Active days: {commit_activity['active_days']}")
    print("Top contributors:")
    for contributor, count in list(commit_activity['top_contributors'].items())[:5]:
        print(f"  {contributor}: {count} commits")
    
    # Issues and PRs
    print("\n3. Issues and Pull Requests:")
    issue_pr_stats = analyzer.get_issue_pr_stats(owner)
    print(f"Total issues: {issue_pr_stats['issues']['total']}")
    print(f"Open issues: {issue_pr_stats['issues']['by_state'].get('open', 0)}")
    print(f"Total PRs: {issue_pr_stats['pull_requests']['total']}")
    print(f"Merged PRs: {issue_pr_stats['pull_requests']['by_state'].get('merged', 0)}")
    
    # Contributors
    print("\n4. Contributors Analysis:")
    contributor_analysis = analyzer.get_contributor_analysis(owner)
    print(f"Total contributors: {contributor_analysis['total_contributors']}")
    print(f"Multi-repo contributors: {contributor_analysis['multi_repo_contributors']}")
    print("Top contributors:")
    for contributor in contributor_analysis['top_contributors'][:5]:
        print(f"  {contributor['login']}: {contributor['total_contributions']} contributions across {contributor['repositories_count']} repos")
    
    # Recent activity
    print("\n5. Recent Activity:")
    recent_activity = analyzer.get_recent_activity(owner, 10)
    print(f"Recent commits: {len(recent_activity['commits'])}")
    print(f"Recent issues: {len(recent_activity['issues'])}")
    print(f"Recent PRs: {len(recent_activity['pull_requests'])}")

if __name__ == "__main__":
    example_usage()