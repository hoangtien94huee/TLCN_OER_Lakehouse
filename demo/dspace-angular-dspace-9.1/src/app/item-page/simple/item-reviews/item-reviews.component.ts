import { CommonModule } from '@angular/common';
import {
  Component,
  Input,
  OnInit,
  ChangeDetectorRef,
  Inject,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { TranslateModule } from '@ngx-translate/core';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { AuthService } from '../../../core/auth/auth.service';
import { EPerson } from '../../../core/eperson/models/eperson.model';
import { Observable, of } from 'rxjs';
import { catchError, take } from 'rxjs/operators';
import { APP_CONFIG, AppConfig } from '../../../../config/app-config.interface';

interface Review {
  id: number;
  resource_id: string;
  eperson_id: string;
  email: string;
  display_name: string;
  firstname?: string;
  lastname?: string;
  rating: number;
  comment: string;
  created_at: string;
  updated_at: string;
  helpful_count: number;
}

interface ReviewStats {
  resource_id: string;
  total_reviews: number;
  average_rating: number;
  distribution: {
    '5': number;
    '4': number;
    '3': number;
    '2': number;
    '1': number;
  };
}

interface ReviewsResponse {
  resource_id: string;
  total: number;
  reviews: Review[];
}

@Component({
  selector: 'ds-item-reviews',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    TranslateModule,
    HttpClientModule,
  ],
  templateUrl: './item-reviews.component.html',
  styleUrls: ['./item-reviews.component.scss'],
})
export class ItemReviewsComponent implements OnInit {
  @Input() itemId: string = '';
  @Input() itemTitle: string = '';

  // API endpoint - configurable via environment
  private apiUrl = 'http://localhost:8088/api';

  // State
  reviews: Review[] = [];
  stats: ReviewStats | null = null;
  userReview: Review | null = null;
  isLoading = true;
  isSubmitting = false;
  showReviewForm = false;
  errorMessage = '';
  successMessage = '';

  // Current user
  currentUser: EPerson | null = null;
  isAuthenticated = false;

  // Review form
  newRating = 5;
  newComment = '';

  // Pagination
  currentPage = 0;
  pageSize = 5;
  totalReviews = 0;

  // Sort
  sortBy = 'created_at';
  sortOrder = 'desc';

  constructor(
    private http: HttpClient,
    private authService: AuthService,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.checkAuthentication();
    this.loadStats();
    this.loadReviews();
  }

  private checkAuthentication(): void {
    this.authService.isAuthenticated().pipe(take(1)).subscribe(isAuth => {
      this.isAuthenticated = isAuth;
      if (isAuth) {
        this.authService.getAuthenticatedUserFromStore().pipe(take(1)).subscribe(user => {
          this.currentUser = user;
          if (user?.email) {
            this.loadUserReview(user.email);
          }
          this.cdr.detectChanges();
        });
      }
    });
  }

  private loadStats(): void {
    this.http.get<ReviewStats>(`${this.apiUrl}/reviews/${this.itemId}/stats`)
      .pipe(catchError(() => of(null)))
      .subscribe(stats => {
        this.stats = stats;
        this.cdr.detectChanges();
      });
  }

  loadReviews(): void {
    this.isLoading = true;
    const offset = this.currentPage * this.pageSize;
    
    this.http.get<ReviewsResponse>(
      `${this.apiUrl}/reviews/${this.itemId}?limit=${this.pageSize}&offset=${offset}&sort_by=${this.sortBy}&order=${this.sortOrder}`
    ).pipe(catchError(() => of({ resource_id: this.itemId, total: 0, reviews: [] })))
      .subscribe(response => {
        this.reviews = response.reviews;
        this.totalReviews = response.total;
        this.isLoading = false;
        this.cdr.detectChanges();
      });
  }

  private loadUserReview(email: string): void {
    this.http.get<{ has_review: boolean; review: Review | null }>(
      `${this.apiUrl}/reviews/${this.itemId}/user?email=${encodeURIComponent(email)}`
    ).pipe(catchError(() => of({ has_review: false, review: null })))
      .subscribe(response => {
        if (response.has_review && response.review) {
          this.userReview = response.review;
          this.newRating = response.review.rating;
          this.newComment = response.review.comment;
        }
        this.cdr.detectChanges();
      });
  }

  submitReview(): void {
    if (!this.currentUser?.email) {
      this.errorMessage = 'Bạn cần đăng nhập để đánh giá';
      return;
    }

    this.isSubmitting = true;
    this.errorMessage = '';
    this.successMessage = '';

    const reviewData = {
      resource_id: this.itemId,
      email: this.currentUser.email,
      rating: this.newRating,
      comment: this.newComment,
    };

    this.http.post<{ success: boolean; message: string; review: Review }>(
      `${this.apiUrl}/reviews`,
      reviewData
    ).subscribe({
      next: (response) => {
        if (response.success) {
          this.userReview = response.review;
          this.successMessage = this.userReview ? 'Đã cập nhật đánh giá!' : 'Đã gửi đánh giá!';
          this.showReviewForm = false;
          this.loadStats();
          this.loadReviews();
        }
        this.isSubmitting = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.errorMessage = err.error?.detail || 'Không thể gửi đánh giá. Vui lòng thử lại.';
        this.isSubmitting = false;
        this.cdr.detectChanges();
      }
    });
  }

  deleteReview(): void {
    if (!this.userReview || !this.currentUser) return;

    if (!confirm('Bạn có chắc muốn xóa đánh giá này?')) return;

    this.http.delete<{ success: boolean }>(
      `${this.apiUrl}/reviews/${this.itemId}/user/${this.userReview.eperson_id}`
    ).subscribe({
      next: () => {
        this.userReview = null;
        this.newRating = 5;
        this.newComment = '';
        this.successMessage = 'Đã xóa đánh giá!';
        this.loadStats();
        this.loadReviews();
        this.cdr.detectChanges();
      },
      error: () => {
        this.errorMessage = 'Không thể xóa đánh giá.';
        this.cdr.detectChanges();
      }
    });
  }

  markHelpful(review: Review): void {
    if (!this.currentUser) {
      this.errorMessage = 'Bạn cần đăng nhập';
      return;
    }

    // Get current user's eperson_id
    this.authService.getAuthenticatedUserFromStore().pipe(take(1)).subscribe(user => {
      if (user?.id) {
        this.http.post<{ success: boolean; message: string }>(
          `${this.apiUrl}/reviews/${review.id}/helpful?eperson_id=${user.id}`,
          {}
        ).subscribe({
          next: (response) => {
            if (response.success) {
              review.helpful_count++;
            }
            this.cdr.detectChanges();
          },
          error: () => {}
        });
      }
    });
  }

  setRating(rating: number): void {
    this.newRating = rating;
  }

  toggleReviewForm(): void {
    this.showReviewForm = !this.showReviewForm;
    this.errorMessage = '';
    this.successMessage = '';
  }

  changePage(page: number): void {
    if (page >= 0 && page < this.totalPages) {
      this.currentPage = page;
      this.loadReviews();
    }
  }

  changeSort(sortBy: string): void {
    if (this.sortBy === sortBy) {
      this.sortOrder = this.sortOrder === 'desc' ? 'asc' : 'desc';
    } else {
      this.sortBy = sortBy;
      this.sortOrder = 'desc';
    }
    this.currentPage = 0;
    this.loadReviews();
  }

  get totalPages(): number {
    return Math.ceil(this.totalReviews / this.pageSize);
  }

  get pagesArray(): number[] {
    return Array.from({ length: this.totalPages }, (_, i) => i);
  }

  getStarsArray(rating: number): number[] {
    return Array.from({ length: 5 }, (_, i) => i + 1);
  }

  getPercentage(count: number): number {
    if (!this.stats || this.stats.total_reviews === 0) return 0;
    return Math.round((count / this.stats.total_reviews) * 100);
  }

  formatDate(dateString: string): string {
    const date = new Date(dateString);
    return date.toLocaleDateString('vi-VN', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  }
}
