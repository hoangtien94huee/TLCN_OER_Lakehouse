import {
  Component,
  OnDestroy,
  ViewChild,
  ElementRef,
  AfterViewChecked,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { TranslateModule } from '@ngx-translate/core';
import { HttpClient, HttpClientModule } from '@angular/common/http';

interface AskSource {
  title: string;
  url: string;
  file: string;
  page: number | null;
  snippet: string;
}

interface AskResponse {
  question: string;
  answer: string;
  sources: AskSource[];
  context_count: number;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  text: string;
  sources?: AskSource[];
  loading?: boolean;
  error?: boolean;
}

@Component({
  selector: 'ds-ask-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterModule, TranslateModule, HttpClientModule],
  templateUrl: './ask-page.component.html',
  styleUrls: ['./ask-page.component.scss'],
})
export class AskPageComponent implements OnDestroy, AfterViewChecked {
  question = '';
  messages: ChatMessage[] = [];
  isLoading = false;
  readonly exampleQuestions = [
    'Đạo hàm là gì và cách tính đạo hàm?',
    'Giải thích định lý Bayes bằng ví dụ thực tế',
    'Cấu trúc dữ liệu nào phù hợp cho bài toán tìm kiếm?',
  ];
  private shouldScrollToBottom = false;

  // OER Search API base URL (search_app runs on port 8088)
  private readonly API_URL = 'http://localhost:8088';

  @ViewChild('chatContainer') private chatContainer!: ElementRef;

  constructor(private http: HttpClient) {}

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  ngOnDestroy(): void {}

  get canSend(): boolean {
    return this.question.trim().length >= 3 && !this.isLoading;
  }

  onEnterKey(event: KeyboardEvent): void {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      if (this.canSend) {
        this.sendQuestion();
      }
    }
  }

  sendQuestion(): void {
    const q = this.question.trim();
    if (!q || this.isLoading) return;

    // Add user message
    this.messages.push({ role: 'user', text: q });

    // Add placeholder assistant message while loading
    const loadingMsg: ChatMessage = { role: 'assistant', text: '', loading: true };
    this.messages.push(loadingMsg);

    this.question = '';
    this.isLoading = true;
    this.shouldScrollToBottom = true;

    this.http
      .post<AskResponse>(`${this.API_URL}/api/ask`, { question: q, top_k: 5 })
      .subscribe({
        next: (res) => {
          const idx = this.messages.lastIndexOf(loadingMsg);
          this.messages[idx] = {
            role: 'assistant',
            text: res.answer,
            sources: res.sources?.filter((s) => s.title),
          };
          this.isLoading = false;
          this.shouldScrollToBottom = true;
        },
        error: (err) => {
          const idx = this.messages.lastIndexOf(loadingMsg);
          const errMsg =
            err.status === 0
              ? 'Không thể kết nối đến dịch vụ hỏi đáp. Vui lòng thử lại sau.'
              : `Lỗi máy chủ (${err.status}). Vui lòng thử lại sau.`;
          this.messages[idx] = { role: 'assistant', text: errMsg, error: true };
          this.isLoading = false;
          this.shouldScrollToBottom = true;
        },
      });
  }

  clearChat(): void {
    this.messages = [];
    this.question = '';
  }

  private scrollToBottom(): void {
    try {
      if (this.chatContainer) {
        this.chatContainer.nativeElement.scrollTop =
          this.chatContainer.nativeElement.scrollHeight;
      }
    } catch {}
  }
}
