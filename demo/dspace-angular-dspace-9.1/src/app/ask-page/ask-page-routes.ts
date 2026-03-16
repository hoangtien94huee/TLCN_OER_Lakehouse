import { Route } from '@angular/router';

import { i18nBreadcrumbResolver } from '../core/breadcrumbs/i18n-breadcrumb.resolver';
import { AskPageComponent } from './ask-page.component';

export const ROUTES: Route[] = [
  {
    path: '',
    component: AskPageComponent,
    resolve: { breadcrumb: i18nBreadcrumbResolver },
    data: { title: 'ask.page.title', breadcrumbKey: 'ask' },
  },
];
