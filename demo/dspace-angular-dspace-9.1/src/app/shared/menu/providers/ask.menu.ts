import { Injectable } from '@angular/core';
import {
  Observable,
  of,
} from 'rxjs';

import { MenuItemType } from '../menu-item-type.model';
import {
  AbstractMenuProvider,
  PartialMenuSection,
} from '../menu-provider.model';

/**
 * Menu provider to add an "Hỏi đáp" (Q&A) link to the public navbar
 */
@Injectable()
export class AskMenuProvider extends AbstractMenuProvider {
  public getSections(): Observable<PartialMenuSection[]> {
    return of([
      {
        visible: true,
        model: {
          type: MenuItemType.LINK,
          text: 'menu.section.ask',
          link: '/ask',
        },
        icon: 'comments',
      },
    ] as PartialMenuSection[]);
  }
}
