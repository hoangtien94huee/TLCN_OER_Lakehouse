import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { TranslateModule } from '@ngx-translate/core';
import { HttpClient } from '@angular/common/http';
import { Subscription } from 'rxjs';
import { AuthService } from '../../core/auth/auth.service';

interface RecommendedItem {
  resource_id: string;
  title: string;
  description: string;
  url: string;
  source: string;
  subjects: string[];
  score: number;
  matched_topics?: string[];
}

interface RecommendationResponse {
  student_id: string;
  topics: { [key: string]: number };
  total: number;
  results: RecommendedItem[];
  message?: string;
}

@Component({
  selector: 'ds-recommendation-list',
  standalone: true,
  imports: [CommonModule, RouterModule, TranslateModule],
  templateUrl: './recommendation-list.component.html',
  styleUrls: ['./recommendation-list.component.scss']
})
export class RecommendationListComponent implements OnInit, OnDestroy {
  recommendations: RecommendedItem[] = [];
  topTopics: string[] = [];
  isLoading = true;
  hasError = false;
  errorMessage = '';
  isLoggedIn = false;
  studentId: string | null = null;

  private subscription: Subscription | null = null;
  private authSubscription: Subscription | null = null;

  // API endpoint - search_app runs on port 8088
  private readonly API_URL = 'http://localhost:8088';

  constructor(
    private http: HttpClient,
    private authService: AuthService
  ) {}

  ngOnInit(): void {
    this.checkLoginAndLoadRecommendations();
  }

  ngOnDestroy(): void {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
    if (this.authSubscription) {
      this.authSubscription.unsubscribe();
    }
  }

  private checkLoginAndLoadRecommendations(): void {
    // Get authenticated user from DSpace AuthService
    this.authSubscription = this.authService.getAuthenticatedUserFromStoreIfAuthenticated()
      .subscribe(user => {
        if (user && user.email) {
          // Extract student ID from email (format: {mssv}@gmail.com)
          const email = user.email;
          if (email.includes('@')) {
            this.studentId = email.split('@')[0];
          } else {
            this.studentId = email;
          }
          this.isLoggedIn = true;
          this.loadRecommendations();
        } else {
          this.isLoggedIn = false;
          this.isLoading = false;
        }
      });
  }

  private loadRecommendations(): void {
    if (!this.studentId) {
      this.isLoading = false;
      return;
    }

    this.isLoading = true;
    this.hasError = false;

    this.subscription = this.http.get<RecommendationResponse>(
      `${this.API_URL}/api/recommend/${this.studentId}?limit=6`
    ).subscribe({
      next: (response) => {
        this.recommendations = response.results || [];
        
        // Get top 2 topics for display
        if (response.topics) {
          const sortedTopics = Object.entries(response.topics)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 2)
            .map(([topic]) => topic);
          this.topTopics = sortedTopics;
        }

        this.isLoading = false;
      },
      error: (error) => {
        console.error('Error loading recommendations:', error);
        this.hasError = true;
        this.errorMessage = 'Unable to load recommendations';
        this.isLoading = false;
      }
    });
  }

  getSourceIcon(source: string): string {
    switch (source?.toLowerCase()) {
      case 'mit_ocw':
        return 'mit_ocw';
      case 'openstax':
        return 'openstax';
      case 'otl':
      case 'open+textbook+library':
        return 'otl';
      default:
        return 'oer';
    }
  }

  getSourceLabel(source: string): string {
    switch (source?.toLowerCase()) {
      case 'mit_ocw':
        return 'MIT OCW';
      case 'openstax':
        return 'OpenStax';
      case 'otl':
      case 'open+textbook+library':
        return 'Open Textbook Library';
      default:
        return source || 'OER';
    }
  }

  truncateText(text: string, maxLength: number = 150): string {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
  }

  openResource(url: string): void {
    if (url && url !== '#') {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  }
}
