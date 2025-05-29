#!/usr/bin/env python3
"""
GitHub API to MongoDB Data Collection Application

This application connects to the GitHub REST API, retrieves various types of data,
and stores them in MongoDB with proper relational mapping and timestamps.

Features:
- Fetch repositories, commits, contributors, pull requests, issues, and comments
- Store data in MongoDB with relational mapping
- Timestamp all records for traceability
- Rate limiting and error handling
- Configurable and modular design

Requirements:
- pip install pymongo requests python-dotenv
"""

import os
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv

import requests
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class GitHubConfig:
    """Configuration for GitHub API access"""
    token: str
    base_url: str = "https://api.github.com"
    requests_per_hour: int = 5000  # GitHub API rate limit
    
class GitHubAPIClient:
    """GitHub API client with rate limiting and error handling"""
    
    def __init__(self, config: GitHubConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {config.token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-MongoDB-Collector/1.0'
        })
        self.rate_limit_remaining = config.requests_per_hour
        self.rate_limit_reset = time.time() + 3600
    
    def _check_rate_limit(self):
        """Check and handle rate limiting"""
        if self.rate_limit_remaining <= 1:
            sleep_time = max(0, self.rate_limit_reset - time.time())
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make a request to GitHub API with error handling"""
        self._check_rate_limit()
        
        url = f"{self.config.base_url}{endpoint}"
        
        try:
            response = self.session.get(url, params=params or {})
            
            # Update rate limit info
            self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time() + 3600))
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    def get_repositories(self, owner: str, per_page: int = 100) -> List[Dict]:
        """Get repositories for a user/organization"""
        repos = []
        page = 1
        
        while True:
            data = self._make_request(
                f"/users/{owner}/repos",
                params={'per_page': per_page, 'page': page, 'sort': 'updated'}
            )
            
            if not data:
                break
                
            repos.extend(data)
            page += 1
            
            if len(data) < per_page:
                break
        
        return repos
    
    def get_commits(self, owner: str, repo: str, per_page: int = 100) -> List[Dict]:
        """Get commits for a repository"""
        commits = []
        page = 1
        
        while True:
            try:
                data = self._make_request(
                    f"/repos/{owner}/{repo}/commits",
                    params={'per_page': per_page, 'page': page}
                )
                
                if not data:
                    break
                    
                commits.extend(data)
                page += 1
                
                if len(data) < per_page:
                    break
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 409:  # Empty repository
                    logger.info(f"Repository {owner}/{repo} is empty")
                    break
                raise
        
        return commits
    
    def get_contributors(self, owner: str, repo: str) -> List[Dict]:
        """Get contributors for a repository"""
        try:
            return self._make_request(f"/repos/{owner}/{repo}/contributors")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Contributors not found for {owner}/{repo}")
                return []
            raise
    
    def get_pull_requests(self, owner: str, repo: str, state: str = 'all') -> List[Dict]:
        """Get pull requests for a repository"""
        prs = []
        page = 1
        
        while True:
            data = self._make_request(
                f"/repos/{owner}/{repo}/pulls",
                params={'state': state, 'per_page': 100, 'page': page}
            )
            
            if not data:
                break
                
            prs.extend(data)
            page += 1
            
            if len(data) < 100:
                break
        
        return prs
    
    def get_issues(self, owner: str, repo: str, state: str = 'all') -> List[Dict]:
        """Get issues for a repository"""
        issues = []
        page = 1
        
        while True:
            data = self._make_request(
                f"/repos/{owner}/{repo}/issues",
                params={'state': state, 'per_page': 100, 'page': page}
            )
            
            if not data:
                break
                
            issues.extend(data)
            page += 1
            
            if len(data) < 100:
                break
        
        return issues
    
    def get_issue_comments(self, owner: str, repo: str, issue_number: int) -> List[Dict]:
        """Get comments for a specific issue"""
        return self._make_request(f"/repos/{owner}/{repo}/issues/{issue_number}/comments")
    
    def get_pr_comments(self, owner: str, repo: str, pr_number: int) -> List[Dict]:
        """Get comments for a specific pull request"""
        return self._make_request(f"/repos/{owner}/{repo}/pulls/{pr_number}/comments")

class MongoDBManager:
    """MongoDB manager for storing GitHub data"""
    
    def __init__(self, connection_string: str, database_name: str):
        try:
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
            
        self.db: Database = self.client[database_name]
        self._setup_collections()
    
    def _setup_collections(self):
        """Setup MongoDB collections with indexes"""
        collections = {
            'repositories': [
                ('owner.login', ASCENDING),
                ('name', ASCENDING),
                ('updated_at', DESCENDING),
                ('collected_at', DESCENDING)
            ],
            'commits': [
                ('repository_id', ASCENDING),
                ('sha', ASCENDING),
                ('commit.author.date', DESCENDING),
                ('collected_at', DESCENDING)
            ],
            'contributors': [
                ('repository_id', ASCENDING),
                ('login', ASCENDING),
                ('collected_at', DESCENDING)
            ],
            'pull_requests': [
                ('repository_id', ASCENDING),
                ('number', ASCENDING),
                ('state', ASCENDING),
                ('created_at', DESCENDING),
                ('collected_at', DESCENDING)
            ],
            'issues': [
                ('repository_id', ASCENDING),
                ('number', ASCENDING),
                ('state', ASCENDING),
                ('created_at', DESCENDING),
                ('collected_at', DESCENDING)
            ],
            'comments': [
                ('repository_id', ASCENDING),
                ('issue_id', ASCENDING),
                ('pull_request_id', ASCENDING),
                ('created_at', DESCENDING),
                ('collected_at', DESCENDING)
            ]
        }
        
        for collection_name, indexes in collections.items():
            collection = self.db[collection_name]
            for index in indexes:
                try:
                    collection.create_index([index])
                except Exception as e:
                    logger.warning(f"Failed to create index {index} on {collection_name}: {e}")
    
    def _add_timestamp(self, document: Dict) -> Dict:
        """Add collection timestamp to document"""
        document['collected_at'] = datetime.now(timezone.utc)
        return document
    
    def store_repositories(self, repositories: List[Dict]) -> List[str]:
        """Store repositories in MongoDB"""
        if not repositories:
            return []
        
        collection: Collection = self.db.repositories
        repo_ids = []
        
        for repo in repositories:
            repo = self._add_timestamp(repo)
            
            # Upsert repository
            result = collection.update_one(
                {'id': repo['id']},
                {'$set': repo},
                upsert=True
            )
            
            repo_ids.append(str(repo['id']))
        
        logger.info(f"Stored {len(repositories)} repositories")
        return repo_ids
    
    def store_commits(self, repository_id: int, commits: List[Dict]) -> List[str]:
        """Store commits in MongoDB"""
        if not commits:
            return []
        
        collection: Collection = self.db.commits
        commit_ids = []
        
        for commit in commits:
            commit = self._add_timestamp(commit)
            commit['repository_id'] = repository_id
            
            # Upsert commit
            result = collection.update_one(
                {'sha': commit['sha'], 'repository_id': repository_id},
                {'$set': commit},
                upsert=True
            )
            
            commit_ids.append(commit['sha'])
        
        logger.info(f"Stored {len(commits)} commits for repository {repository_id}")
        return commit_ids
    
    def store_contributors(self, repository_id: int, contributors: List[Dict]) -> List[str]:
        """Store contributors in MongoDB"""
        if not contributors:
            return []
        
        collection: Collection = self.db.contributors
        contributor_ids = []
        
        for contributor in contributors:
            contributor = self._add_timestamp(contributor)
            contributor['repository_id'] = repository_id
            
            # Upsert contributor
            result = collection.update_one(
                {'login': contributor['login'], 'repository_id': repository_id},
                {'$set': contributor},
                upsert=True
            )
            
            contributor_ids.append(contributor['login'])
        
        logger.info(f"Stored {len(contributors)} contributors for repository {repository_id}")
        return contributor_ids
    
    def store_pull_requests(self, repository_id: int, pull_requests: List[Dict]) -> List[int]:
        """Store pull requests in MongoDB"""
        if not pull_requests:
            return []
        
        collection: Collection = self.db.pull_requests
        pr_ids = []
        
        for pr in pull_requests:
            pr = self._add_timestamp(pr)
            pr['repository_id'] = repository_id
            
            # Upsert pull request
            result = collection.update_one(
                {'id': pr['id']},
                {'$set': pr},
                upsert=True
            )
            
            pr_ids.append(pr['number'])
        
        logger.info(f"Stored {len(pull_requests)} pull requests for repository {repository_id}")
        return pr_ids
    
    def store_issues(self, repository_id: int, issues: List[Dict]) -> List[int]:
        """Store issues in MongoDB"""
        if not issues:
            return []
        
        collection: Collection = self.db.issues
        issue_ids = []
        
        for issue in issues:
            # Skip pull requests (GitHub API returns PRs as issues)
            if 'pull_request' in issue:
                continue
                
            issue = self._add_timestamp(issue)
            issue['repository_id'] = repository_id
            
            # Upsert issue
            result = collection.update_one(
                {'id': issue['id']},
                {'$set': issue},
                upsert=True
            )
            
            issue_ids.append(issue['number'])
        
        logger.info(f"Stored {len(issue_ids)} issues for repository {repository_id}")
        return issue_ids
    
    def store_comments(self, repository_id: int, comments: List[Dict], 
                      issue_id: Optional[int] = None, 
                      pull_request_id: Optional[int] = None) -> List[int]:
        """Store comments in MongoDB"""
        if not comments:
            return []
        
        collection: Collection = self.db.comments
        comment_ids = []
        
        for comment in comments:
            comment = self._add_timestamp(comment)
            comment['repository_id'] = repository_id
            
            if issue_id:
                comment['issue_id'] = issue_id
            if pull_request_id:
                comment['pull_request_id'] = pull_request_id
            
            # Upsert comment
            result = collection.update_one(
                {'id': comment['id']},
                {'$set': comment},
                upsert=True
            )
            
            comment_ids.append(comment['id'])
        
        logger.info(f"Stored {len(comments)} comments for repository {repository_id}")
        return comment_ids

class GitHubDataCollector:
    """Main data collector class"""
    
    def __init__(self, github_config: GitHubConfig, mongodb_manager: MongoDBManager):
        self.github_client = GitHubAPIClient(github_config)
        self.mongodb_manager = mongodb_manager
    
    def collect_all_data(self, owner: str, include_comments: bool = True):
        """Collect all data for a GitHub user/organization"""
        logger.info(f"Starting data collection for {owner}")
        
        # Get repositories
        repositories = self.github_client.get_repositories(owner)
        repo_ids = self.mongodb_manager.store_repositories(repositories)
        
        # Process each repository
        for repo in repositories:
            repo_name = repo['name']
            repo_id = repo['id']
            
            logger.info(f"Processing repository: {owner}/{repo_name}")
            
            # Get commits
            commits = self.github_client.get_commits(owner, repo_name)
            self.mongodb_manager.store_commits(repo_id, commits)
            
            # Get contributors
            contributors = self.github_client.get_contributors(owner, repo_name)
            self.mongodb_manager.store_contributors(repo_id, contributors)
            
            # Get pull requests
            pull_requests = self.github_client.get_pull_requests(owner, repo_name)
            pr_ids = self.mongodb_manager.store_pull_requests(repo_id, pull_requests)
            
            # Get issues
            issues = self.github_client.get_issues(owner, repo_name)
            issue_ids = self.mongodb_manager.store_issues(repo_id, issues)
            
            # Get comments if requested
            if include_comments:
                # Issue comments
                for issue_number in issue_ids:
                    comments = self.github_client.get_issue_comments(owner, repo_name, issue_number)
                    self.mongodb_manager.store_comments(repo_id, comments, issue_id=issue_number)
                
                # PR comments
                for pr_number in pr_ids:
                    comments = self.github_client.get_pr_comments(owner, repo_name, pr_number)
                    self.mongodb_manager.store_comments(repo_id, comments, pull_request_id=pr_number)
        
        logger.info(f"Completed data collection for {owner}")

def main():
    """Main function"""
    # Configuration
    github_token = os.getenv('GITHUB_TOKEN')
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    database_name = os.getenv('DATABASE_NAME', 'github_data')
    #owner = os.getenv('GITHUB_OWNER', 'letta')  # Default to octocat for testing
    owner = "letta"
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required")
    
    logger.info(f"Connecting to MongoDB: {mongodb_uri}")
    logger.info(f"Target GitHub owner: {owner}")
    logger.info(f"Target GitHub db: {database_name}")
    
    # Initialize components
    github_config = GitHubConfig(token=github_token)
    mongodb_manager = MongoDBManager(mongodb_uri, database_name)
    collector = GitHubDataCollector(github_config, mongodb_manager)
    
    # Collect data
    try:
        collector.collect_all_data(owner, include_comments=True)
        logger.info("Data collection completed successfully")
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
        raise

if __name__ == "__main__":
    main()