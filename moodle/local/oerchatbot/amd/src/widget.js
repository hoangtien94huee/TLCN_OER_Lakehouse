define([], function() {
    function el(tag, attrs, text) {
        var node = document.createElement(tag);
        if (attrs) {
            Object.keys(attrs).forEach(function(k) {
                node.setAttribute(k, attrs[k]);
            });
        }
        if (text) {
            node.textContent = text;
        }
        return node;
    }

    function ensureStyles(position) {
        if (document.getElementById('oerchatbot-style')) {
            return;
        }
        var side = position === 'left' ? 'left' : 'right';
        var style = el('style', {id: 'oerchatbot-style'});
        style.textContent = ''
            + '.oerchatbot-btn{position:fixed;bottom:18px;' + side + ':18px;z-index:9999;background:linear-gradient(135deg,#0f6cbf,#2a8bf2);color:#fff;border:none;border-radius:999px;padding:12px 16px;cursor:pointer;font-weight:700;box-shadow:0 8px 20px rgba(15,108,191,.32);display:flex;align-items:center;gap:8px;transition:transform .18s ease,box-shadow .18s ease;}'
            + '.oerchatbot-btn:hover{transform:translateY(-1px);box-shadow:0 12px 24px rgba(15,108,191,.38);}'
            + '.oerchatbot-btn:focus{outline:2px solid #9dc9ff;outline-offset:2px;}'
            + '.oerchatbot-btn-icon{font-size:16px;line-height:1;}'
            + '.oerchatbot-box{position:fixed;bottom:72px;' + side + ':18px;width:min(390px,calc(100vw - 24px));height:min(600px,calc(100vh - 90px));z-index:9999;background:#fff;border:1px solid #d9e4ef;border-radius:16px;display:none;box-shadow:0 16px 42px rgba(15,23,42,.20);overflow:hidden;flex-direction:column;}'
            + '.oerchatbot-head{padding:12px 14px;background:linear-gradient(135deg,#0f6cbf,#2a8bf2);color:#fff;display:flex;align-items:center;justify-content:space-between;}'
            + '.oerchatbot-head-left{min-width:0;}'
            + '.oerchatbot-title{font-weight:700;line-height:1.15;}'
            + '.oerchatbot-subtitle{font-size:12px;opacity:.92;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px;}'
            + '.oerchatbot-head-right{display:flex;align-items:center;gap:8px;}'
            + '.oerchatbot-status{display:none;}'
            + '.oerchatbot-close{border:none;background:rgba(255,255,255,.2);color:#fff;border-radius:8px;width:24px;height:24px;cursor:pointer;font-size:15px;line-height:1;}'
            + '.oerchatbot-msgs{flex:1;overflow:auto;padding:12px;background:linear-gradient(180deg,#f8fbff,#f5f8fb);font-size:14px;}'
            + '.oerchatbot-msg{margin-bottom:12px;display:flex;flex-direction:column;}'
            + '.oerchatbot-msg-user{align-items:flex-end;}'
            + '.oerchatbot-msg-bot{align-items:flex-start;}'
            + '.oerchatbot-msg-meta{font-size:11px;color:#6b7280;margin-bottom:4px;padding:0 6px;}'
            + '.oerchatbot-msg-bubble{max-width:88%;border:1px solid #dce7f2;border-radius:12px;padding:9px 10px;line-height:1.5;word-break:break-word;background:#fff;box-shadow:0 2px 10px rgba(15,23,42,.05);}'
            + '.oerchatbot-msg-user .oerchatbot-msg-bubble{background:#eaf3ff;border-color:#cde1fb;}'
            + '.oerchatbot-msg-line{margin:2px 0;}'
            + '.oerchatbot-msg-item{display:flex;gap:6px;align-items:flex-start;margin:3px 0;}'
            + '.oerchatbot-msg-item-num{min-width:18px;color:#0f6cbf;font-weight:700;}'
            + '.oerchatbot-msg-item-bullet{min-width:14px;color:#0f6cbf;font-weight:700;}'
            + '.oerchatbot-msg-bubble a{color:#0f6cbf;text-decoration:none;}'
            + '.oerchatbot-msg-bubble a:hover{text-decoration:underline;}'
            + '.oerchatbot-suggestions{display:flex;flex-wrap:wrap;gap:8px;margin:4px 0 8px 0;}'
            + '.oerchatbot-suggestion{border:1px solid #cfe1f5;background:#fff;border-radius:999px;padding:6px 10px;font-size:12px;color:#0f6cbf;cursor:pointer;}'
            + '.oerchatbot-suggestion:hover{background:#eef6ff;}'
            + '.oerchatbot-row{display:flex;gap:8px;padding:10px;border-top:1px solid #e4edf6;background:#fff;}'
            + '.oerchatbot-row textarea{flex:1;resize:none;height:70px;border:1px solid #d2dde9;border-radius:10px;padding:8px 10px;font:inherit;line-height:1.45;}'
            + '.oerchatbot-row textarea:focus{outline:2px solid #c7e0ff;border-color:#0f6cbf;}'
            + '.oerchatbot-send{background:#0f6cbf;color:#fff;border:none;padding:10px 13px;border-radius:10px;cursor:pointer;font-weight:600;align-self:flex-end;min-width:58px;}'
            + '.oerchatbot-send:disabled{opacity:.6;cursor:not-allowed;}'
            + '.oerchatbot-hint{font-size:11px;color:#6b7280;padding:0 12px 10px;background:#fff;}'
            + '.oerchatbot-typing{display:flex;gap:4px;align-items:center;}'
            + '.oerchatbot-typing span{width:6px;height:6px;border-radius:50%;background:#5f82ad;animation:oerchatbot-bounce 1.2s infinite ease-in-out;}'
            + '.oerchatbot-typing span:nth-child(2){animation-delay:-.2s;}'
            + '.oerchatbot-typing span:nth-child(3){animation-delay:-.4s;}'
            + '@keyframes oerchatbot-bounce{0%,80%,100%{transform:scale(0);}40%{transform:scale(1);}}'
            + '.oerchatbot-sources{margin-top:8px;padding-top:8px;border-top:1px solid #e4edf6;}'
            + '.oerchatbot-sources-title{font-size:11px;font-weight:600;color:#6b7280;margin-bottom:4px;}'
            + '.oerchatbot-source{display:block;background:#f8fbff;border:1px solid #e4edf6;border-radius:8px;padding:6px 8px;margin-bottom:4px;text-decoration:none;color:inherit;transition:background .15s;font-size:12px;}'
            + '.oerchatbot-source:hover{background:#eef6ff;text-decoration:none;}'
            + '.oerchatbot-source-title{font-weight:600;color:#0f6cbf;}'
            + '.oerchatbot-source-page{font-size:11px;background:#dee2e6;color:#495057;border-radius:10px;padding:1px 6px;margin-left:4px;}'
            + '.oerchatbot-source-section{font-size:11px;color:#6b7280;margin-left:4px;}'
            + '.oerchatbot-source-pdf{font-size:11px;color:#fff;background:#0f6cbf;border-radius:10px;padding:1px 8px;margin-left:6px;text-decoration:none;}'
            + '.oerchatbot-source-pdf:hover{opacity:.85;}'
            + '@media (max-width:680px){.oerchatbot-box{width:calc(100vw - 12px);height:calc(100vh - 80px);bottom:10px;' + side + ':6px;border-radius:14px;}.oerchatbot-btn{bottom:10px;' + side + ':10px;padding:10px 12px;}}';
        document.head.appendChild(style);
    }

    function nowHm() {
        var d = new Date();
        var hh = String(d.getHours()).padStart(2, '0');
        var mm = String(d.getMinutes()).padStart(2, '0');
        return hh + ':' + mm;
    }

    function escapeHtml(text) {
        return String(text || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function linkify(text) {
        return text.replace(/(https?:\/\/[^\s<]+)/g, '<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>');
    }

    function cleanAnswerText(text) {
        if (!text) { return ''; }
        var t = text;
        t = t.replace(/\n?\s*3\)\s*(Nguồn|Sources?)\s*[:：].*/is, '');
        t = t.replace(/^1\)\s*(Trả lời|Answer)\s*[:：]\s*/im, '**Định nghĩa:**\n');
        t = t.replace(/^2\)\s*(Chi tiết|Details?)\s*[:：]\s*/im, '\n**Chi tiết:**\n');
        return t.trim();
    }

    function appendSources(container, sources) {
        if (!sources || !sources.length) { return; }
        var validSources = sources.filter(function(s) { return s && s.title && s.url; });
        if (!validSources.length) { return; }
        var wrap = el('div', {'class': 'oerchatbot-sources'});
        var title = el('div', {'class': 'oerchatbot-sources-title'}, '📚 Nguồn tham khảo (' + validSources.length + ')');
        wrap.appendChild(title);
        validSources.forEach(function(src) {
            var item = el('a', {'class': 'oerchatbot-source', 'href': src.url, 'target': '_blank', 'rel': 'noopener noreferrer'});
            var html = '<span class="oerchatbot-source-title">📄 ' + escapeHtml(src.title) + '</span>';
            if (src.page) { html += '<span class="oerchatbot-source-page">tr. ' + src.page + '</span>'; }
            if (src.section) { html += '<span class="oerchatbot-source-section">| ' + escapeHtml(src.section) + '</span>'; }
            html += '<span class="oerchatbot-source-pdf">Xem PDF</span>';
            item.innerHTML = html;
            item.addEventListener('click', function(evt) {
                // Open from a direct user gesture to avoid iframe/frame navigation restrictions.
                evt.preventDefault();
                var targetUrl = String(src.url || '').trim();
                if (!targetUrl) { return; }
                try {
                    var opened = window.open(targetUrl, '_blank', 'noopener,noreferrer');
                    if (!opened) {
                        window.location.href = targetUrl;
                    }
                } catch (e) {
                    window.location.href = targetUrl;
                }
            });
            wrap.appendChild(item);
        });
        container.appendChild(wrap);
    }

    function formatMessageText(text) {
        var safe = escapeHtml(text || '').replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        var lines = safe.split(/\r?\n/);
        var out = [];
        lines.forEach(function(line) {
            var trimmed = line.trim();
            if (!trimmed) {
                out.push('<div class="oerchatbot-msg-line">&nbsp;</div>');
                return;
            }
            var numbered = trimmed.match(/^(\d+)\.\s+(.*)$/);
            if (numbered) {
                out.push(
                    '<div class="oerchatbot-msg-item">'
                    + '<span class="oerchatbot-msg-item-num">' + numbered[1] + '.</span>'
                    + '<span>' + linkify(numbered[2]) + '</span>'
                    + '</div>'
                );
                return;
            }
            var bullet = trimmed.match(/^-\s+(.*)$/);
            if (bullet) {
                out.push(
                    '<div class="oerchatbot-msg-item">'
                    + '<span class="oerchatbot-msg-item-bullet">•</span>'
                    + '<span>' + linkify(bullet[1]) + '</span>'
                    + '</div>'
                );
                return;
            }
            out.push('<div class="oerchatbot-msg-line">' + linkify(trimmed) + '</div>');
        });
        return out.join('');
    }

    function appendMessage(container, who, text, sources) {
        var wrap = el('div');
        wrap.className = 'oerchatbot-msg ' + (who === 'user' ? 'oerchatbot-msg-user' : 'oerchatbot-msg-bot');

        var meta = el('div', {'class': 'oerchatbot-msg-meta'});
        meta.textContent = (who === 'user' ? 'Bạn' : 'Trợ lý OER') + ' · ' + nowHm();
        var bubble = el('div', {'class': 'oerchatbot-msg-bubble'});
        var displayText = (who === 'bot') ? cleanAnswerText(text) : text;
        bubble.innerHTML = formatMessageText(displayText);
        if (who === 'bot' && sources && sources.length) {
            appendSources(bubble, sources);
        }

        wrap.appendChild(meta);
        wrap.appendChild(bubble);
        container.appendChild(wrap);
        container.scrollTop = container.scrollHeight;
        return wrap;
    }

    function appendTyping(container) {
        var wrap = el('div');
        wrap.className = 'oerchatbot-msg oerchatbot-msg-bot';
        wrap.id = 'oerchatbot-typing-msg';

        var meta = el('div', {'class': 'oerchatbot-msg-meta'}, 'Trợ lý OER · đang trả lời...');
        var bubble = el('div', {'class': 'oerchatbot-msg-bubble'});
        bubble.innerHTML = '<div class="oerchatbot-typing"><span></span><span></span><span></span></div>';
        wrap.appendChild(meta);
        wrap.appendChild(bubble);
        container.appendChild(wrap);
        container.scrollTop = container.scrollHeight;
    }

    function removeTyping(container) {
        var existing = container.querySelector('#oerchatbot-typing-msg');
        if (existing) {
            existing.remove();
        }
    }

    function detectVisibleSection() {
        var hash = window.location.hash || '';
        var hashMatch = hash.match(/^#section-(\d+)$/);
        if (hashMatch) {
            var sectionEl = document.getElementById('section-' + hashMatch[1]);
            if (sectionEl) {
                var nameEl = sectionEl.querySelector('.sectionname .inplaceeditable, .sectionname a, .sectionname span, h3.sectionname');
                var name = nameEl ? (nameEl.textContent || '').trim() : '';
                return {num: parseInt(hashMatch[1], 10), name: name || ('Topic ' + hashMatch[1])};
            }
        }
        var sections = document.querySelectorAll('li.section[id^="section-"]');
        if (!sections.length) {
            return null;
        }
        var viewportMid = window.innerHeight / 2;
        var best = null;
        var bestDist = Infinity;
        sections.forEach(function(sec) {
            var rect = sec.getBoundingClientRect();
            if (rect.bottom < 0 || rect.top > window.innerHeight) {
                return;
            }
            var dist = Math.abs(rect.top + rect.height / 2 - viewportMid);
            if (dist < bestDist) {
                bestDist = dist;
                best = sec;
            }
        });
        if (!best) {
            return null;
        }
        var idMatch = (best.id || '').match(/^section-(\d+)$/);
        if (!idMatch) {
            return null;
        }
        var num = parseInt(idMatch[1], 10);
        if (num <= 0) {
            return null;
        }
        var nameEl = best.querySelector('.sectionname .inplaceeditable, .sectionname a, .sectionname span, h3.sectionname');
        var name = nameEl ? (nameEl.textContent || '').trim() : '';
        return {num: num, name: name || ('Topic ' + num)};
    }

    function getContext(config) {
        var sectionId = config.sectionNum || null;
        var sectionName = config.sectionName || null;
        if (!sectionId && config.hasCourseContext) {
            var detected = detectVisibleSection();
            if (detected) {
                sectionId = detected.num;
                sectionName = detected.name;
            }
        }
        if (!sectionName && sectionId) {
            sectionName = 'Section ' + sectionId;
        }
        return {
            course_id: config.courseId || null,
            course_name: config.courseName || null,
            section_id: sectionId,
            section_name: sectionName,
            activity_id: config.activityId || config.cmId || null,
            activity_name: config.activityName || null,
            role: config.userRole || null,
            page_url: config.pageUrl || window.location.href
        };
    }

    function getAutoGreeting(config) {
        var hasCourseContext = !!config.hasCourseContext;
        var course = (config.courseName || '').trim();
        var section = (config.sectionName || '').trim();
        var activity = (config.activityName || '').trim();
        if (hasCourseContext && course && section && activity) {
            return 'Mình thấy bạn đang học "' + course + '" · "' + section + '" · "' + activity + '". Mình có thể gợi ý tài liệu, giải thích khái niệm, hoặc tóm tắt nhanh nội dung học phần.';
        }
        if (hasCourseContext && course && section) {
            return 'Bạn đang ở môn "' + course + '", phần "' + section + '". Mình có thể gợi ý tài liệu và giải thích các khái niệm trọng tâm cho phần này.';
        }
        if (hasCourseContext && course) {
            return 'Bạn đang ở môn "' + course + '". Bạn có thể hỏi mình gợi ý tài liệu tham khảo hoặc giải thích khái niệm đang học.';
        }
        return 'Xin chào! Mình là trợ lý học tập OER. Bạn có thể hỏi định nghĩa, gợi ý tài liệu, hoặc yêu cầu tóm tắt kiến thức.';
    }

    function getSuggestionButtons(config) {
        if (config.hasCourseContext && config.courseName) {
            return [
                'Gợi ý tài liệu cho môn này',
                'Giải thích khái niệm chính của bài này',
                'Tóm tắt nhanh nội dung cần nhớ'
            ];
        }
        return [
            'Gợi ý tài liệu nhập môn',
            'Giải thích khái niệm cơ bản',
            'Cho tôi lộ trình học ngắn gọn'
        ];
    }

    function appendSuggestions(container, items, onPick) {
        var row = el('div', {'class': 'oerchatbot-suggestions'});
        items.forEach(function(item) {
            var btn = el('button', {'type': 'button', 'class': 'oerchatbot-suggestion'}, item);
            btn.addEventListener('click', function() {
                onPick(item);
            });
            row.appendChild(btn);
        });
        container.appendChild(row);
        container.scrollTop = container.scrollHeight;
    }

    function healthUrlFromApiUrl(apiUrl) {
        var url = String(apiUrl || '').trim();
        if (url.endsWith('/ask')) {
            return url.slice(0, -4) + 'health';
        }
        return url;
    }

    function trimTrailingSlash(url) {
        return String(url || '').replace(/\/+$/, '');
    }

    function buildApiCandidates(apiUrl) {
        var raw = String(apiUrl || '').trim();
        var candidates = [];
        var seen = {};

        function addCandidate(url) {
            var normalized = trimTrailingSlash(url);
            if (!normalized || seen[normalized]) {
                return;
            }
            seen[normalized] = true;
            candidates.push(normalized);
        }

        if (raw) {
            addCandidate(raw);
            addCandidate(raw.replace('127.0.0.1', window.location.hostname || 'localhost'));
            addCandidate(raw.replace('localhost', window.location.hostname || 'localhost'));
            addCandidate(raw.replace('127.0.0.1', 'host.docker.internal'));
            addCandidate(raw.replace('localhost', 'host.docker.internal'));
        }

        var protocol = window.location.protocol || 'http:';
        var hostname = window.location.hostname || 'localhost';
        addCandidate(protocol + '//' + hostname + ':18088/api/ask');

        return candidates;
    }

    function fetchWithTimeout(url, options, timeoutMs) {
        var controller = new AbortController();
        var timer = setTimeout(function() {
            controller.abort();
        }, timeoutMs || 12000);
        var opts = Object.assign({}, options || {}, {signal: controller.signal});
        return fetch(url, opts).finally(function() {
            clearTimeout(timer);
        });
    }

    function checkHealth(config, statusNode) {
        var healthUrl = healthUrlFromApiUrl(config.apiUrl);
        if (!healthUrl) {
            statusNode.className = 'oerchatbot-status offline';
            statusNode.title = 'Chưa cấu hình API URL';
            statusNode.querySelector('.oerchatbot-status-text').textContent = 'Offline';
            return;
        }
        fetch(healthUrl, {method: 'GET'})
            .then(function(resp) {
                if (!resp.ok) {
                    throw new Error('API lỗi');
                }
                return resp.json();
            })
            .then(function() {
                statusNode.className = 'oerchatbot-status online';
                statusNode.title = 'Đã kết nối API';
                statusNode.querySelector('.oerchatbot-status-text').textContent = 'Online';
            })
            .catch(function() {
                statusNode.className = 'oerchatbot-status offline';
                statusNode.title = 'Mất kết nối API';
                statusNode.querySelector('.oerchatbot-status-text').textContent = 'Offline';
            });
    }

    function init(config) {
        ensureStyles(config.position || 'right');

        var btn = el('button', {'type': 'button', 'class': 'oerchatbot-btn', 'aria-label': 'Mở chatbot OER'});
        btn.innerHTML = '<span class="oerchatbot-btn-icon">🎓</span><span>' + escapeHtml(config.title || 'OER Chatbot') + '</span>';

        var box = el('div', {'class': 'oerchatbot-box'});
        var head = el('div', {'class': 'oerchatbot-head'});
        var headLeft = el('div', {'class': 'oerchatbot-head-left'});
        var title = el('div', {'class': 'oerchatbot-title'}, config.title || 'OER Chatbot');
        var subtitleText = (config.courseName || '').trim() ? ('Môn học: ' + config.courseName) : 'Trợ lý học tập OER';
        var subtitle = el('div', {'class': 'oerchatbot-subtitle'}, subtitleText);
        headLeft.appendChild(title);
        headLeft.appendChild(subtitle);

        var headRight = el('div', {'class': 'oerchatbot-head-right'});
        var closeBtn = el('button', {'type': 'button', 'class': 'oerchatbot-close', 'aria-label': 'Đóng chatbot'}, '×');
        headRight.appendChild(closeBtn);

        head.appendChild(headLeft);
        head.appendChild(headRight);

        var msgs = el('div', {'class': 'oerchatbot-msgs'});
        var row = el('div', {'class': 'oerchatbot-row'});
        var input = el('textarea', {'placeholder': 'Đặt câu hỏi... ví dụ: "Cơ sở dữ liệu là gì?"'});
        var send = el('button', {'type': 'button', 'class': 'oerchatbot-send'}, 'Gửi');
        var hint = el('div', {'class': 'oerchatbot-hint'}, 'Nhấn Enter để gửi, Shift + Enter để xuống dòng.');

        row.appendChild(input);
        row.appendChild(send);
        box.appendChild(head);
        box.appendChild(msgs);
        box.appendChild(row);
        box.appendChild(hint);
        document.body.appendChild(btn);
        document.body.appendChild(box);

        var greeted = false;

        function openBox() {
            box.style.display = 'flex';
            if (!greeted) {
                appendMessage(msgs, 'bot', getAutoGreeting(config));
                appendSuggestions(msgs, getSuggestionButtons(config), function(suggestion) {
                    sendQuestion(suggestion);
                });
                greeted = true;
            }
            input.focus();
        }

        function closeBox() {
            box.style.display = 'none';
        }

        function toggleBox() {
            if (box.style.display === 'flex') {
                closeBox();
            } else {
                openBox();
            }
        }

        function setSendingState(isSending) {
            send.disabled = isSending;
            input.disabled = isSending;
            send.textContent = isSending ? '...' : 'Gửi';
        }

        function sendQuestion(question) {
            var q = String(question || '').trim();
            if (!q) {
                return;
            }
            appendMessage(msgs, 'user', q);
            input.value = '';
            setSendingState(true);
            appendTyping(msgs);

            var payload = getContext(config);
            payload.question = q;
            payload.top_k = 5;
            payload.language = 'vi';

            var headers = {'Content-Type': 'application/json'};
            if (config.apiKey) {
                headers['X-API-Key'] = config.apiKey;
            }

            var apiCandidates = buildApiCandidates(config.apiUrl);

            function tryApiAt(index, lastError) {
                if (index >= apiCandidates.length) {
                    throw lastError || new Error('Không tìm thấy endpoint API khả dụng');
                }

                var currentApiUrl = apiCandidates[index];
                return fetchWithTimeout(currentApiUrl, {
                    method: 'POST',
                    headers: headers,
                    body: JSON.stringify(payload)
                }, 15000).then(function(resp) {
                    if (!resp.ok) {
                        return resp.json()
                            .then(function(err) {
                                throw new Error((err && (err.detail || err.message)) ? (err.detail || err.message) : ('HTTP ' + resp.status));
                            })
                            .catch(function() {
                                throw new Error('HTTP ' + resp.status);
                            });
                    }
                    // Persist a working endpoint to reduce future retries.
                    if (currentApiUrl !== config.apiUrl) {
                        config.apiUrl = currentApiUrl;
                    }
                    return resp.json();
                }).catch(function(err) {
                    // Retry only for network/connection type failures.
                    var message = String((err && err.message) || '');
                    var shouldRetry =
                        message.indexOf('Failed to fetch') !== -1 ||
                        message.indexOf('NetworkError') !== -1 ||
                        message.indexOf('ERR_CONNECTION_REFUSED') !== -1 ||
                        message.indexOf('aborted') !== -1;
                    if (shouldRetry) {
                        return tryApiAt(index + 1, err);
                    }
                    throw err;
                });
            }

            tryApiAt(0).then(function(data) {
                var answer = (data && data.answer) ? data.answer : 'Mình chưa có câu trả lời phù hợp. Bạn thử diễn đạt lại chi tiết hơn nhé.';
                var sources = (data && data.sources) ? data.sources : [];
                appendMessage(msgs, 'bot', answer, sources);
            }).catch(function(err) {
                appendMessage(msgs, 'bot', 'Kết nối tạm thời gián đoạn: ' + err.message + '. Bạn thử gửi lại sau ít giây nhé.');
            }).finally(function() {
                removeTyping(msgs);
                setSendingState(false);
                input.focus();
            });
        }

        btn.addEventListener('click', toggleBox);
        closeBtn.addEventListener('click', closeBox);
        send.addEventListener('click', function() {
            sendQuestion(input.value);
        });
        input.addEventListener('keydown', function(evt) {
            if (evt.key === 'Enter' && !evt.shiftKey) {
                evt.preventDefault();
                sendQuestion(input.value);
            }
        });
    }

    return {init: init};
});
