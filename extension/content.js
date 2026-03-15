/**
 * VkusVill Quality Ratings — Content Script
 */

const API_BASE = 'http://localhost:8000';
const BADGE_DATA_ATTR = 'data-vv-rating-badge';
const PROCESSED_ATTR = 'data-vv-rating-processed';
const SORT_BTN_ATTR = 'data-vv-sort-btn';

function getBadgeColor(score) {
  const s = Math.min(100, Math.max(0, Math.round(Number(score))));
  const tier = Math.min(9, Math.floor(s / 10));
  return 'tier' + tier;
}

async function fetchBadgesBatch(urls) {
  const unique = [...new Set(urls.filter((u) => u && u.includes('vkusvill.ru/goods/')))];
  if (unique.length === 0) return {};
  try {
    const res = await fetch(`${API_BASE}/get_badges_batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(unique),
    });
    if (!res.ok) return {};
    const data = await res.json();
    return data.badges || {};
  } catch (e) {
    return {};
  }
}

async function fetchRatingsBatch(names) {
  const unique = [...new Set(names.map((n) => (n || '').trim()).filter(Boolean))];
  if (unique.length === 0) return {};
  try {
    const res = await fetch(`${API_BASE}/get_ratings_batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(unique),
    });
    if (!res.ok) return {};
    const data = await res.json();
    return data.ratings || {};
  } catch (e) {
    console.warn('[VV Ratings] API error:', e);
    return {};
  }
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// Каталог: бейдж оценки + плашки «Лучший»/«Выгодный» + tooltip
function createCatalogBadge(score, pros, cons, badges = {}) {
  const hasScore = score != null && score !== '';
  const s = hasScore ? Math.round(Number(score)) : null;
  const color = hasScore ? getBadgeColor(s) : 'no-data';
  const prosText = (pros || '').trim() || '—';
  const consText = (cons || '').trim() || '—';
  const isBest = badges?.is_best;
  const isValue = badges?.is_value;

  const wrap = document.createElement('div');
  wrap.className = 'vv-catalog-badge-wrap';

  if (isBest || isValue) {
    const pills = document.createElement('div');
    pills.className = 'vv-badge-pills';
    if (isBest) {
      const best = document.createElement('span');
      best.className = 'vv-badge-pill vv-badge-pill--best';
      best.textContent = 'Лучший товар';
      pills.appendChild(best);
    }
    if (isValue) {
      const val = document.createElement('span');
      val.className = 'vv-badge-pill vv-badge-pill--value';
      val.textContent = 'Выгодный';
      pills.appendChild(val);
    }
    wrap.appendChild(pills);
  }

  const badge = document.createElement('span');
  badge.className = `vv-rating-badge vv-rating-badge--${color} vv-rating-badge--catalog`;
  badge.setAttribute(BADGE_DATA_ATTR, '');
  badge.textContent = hasScore ? `${s}/100` : '—';
  wrap.appendChild(badge);

  const tooltip = document.createElement('div');
  tooltip.className = 'vv-catalog-tooltip';
  tooltip.innerHTML = hasScore
    ? `<div class="vv-tooltip-pros"><span class="vv-tooltip-label">Плюсы</span> ${escapeHtml(prosText)}</div>
       <div class="vv-tooltip-cons"><span class="vv-tooltip-label">Минусы</span> ${escapeHtml(consText)}</div>`
    : '<div class="vv-tooltip-pros">Нет данных об оценке</div>';
  wrap.appendChild(tooltip);

  let hoverTimer;
  badge.addEventListener('mouseenter', () => {
    hoverTimer = setTimeout(() => {
      tooltip.classList.add('vv-catalog-tooltip--visible');
      wrap.classList.add('vv-tooltip-visible');
    }, 150);
  });
  badge.addEventListener('mouseleave', () => {
    clearTimeout(hoverTimer);
    tooltip.classList.remove('vv-catalog-tooltip--visible');
    wrap.classList.remove('vv-tooltip-visible');
  });

  return wrap;
}

const SVG_PLUS = '<svg class="vv-icon-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>';
const SVG_MINUS = '<svg class="vv-icon-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="5" y1="12" x2="19" y2="12"/></svg>';

async function fetchProductExtended(url, name) {
  try {
    const u = encodeURIComponent(url || '');
    const n = encodeURIComponent(name || '');
    const res = await fetch(`${API_BASE}/get_product_extended?url=${u}&name=${n}`);
    if (!res.ok) return null;
    return await res.json();
  } catch (e) {
    console.warn('[VV Ratings] Extended API error:', e);
    return null;
  }
}

async function fetchProductImageUrl(productUrl) {
  if (!productUrl || !productUrl.includes('vkusvill.ru/goods/')) return null;
  try {
    const res = await fetch(productUrl, { credentials: 'same-origin' });
    if (!res.ok) return null;
    const html = await res.text();
    const ogMatch = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i)
      || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i);
    if (ogMatch && ogMatch[1]) {
      let u = ogMatch[1].trim();
      if (u.startsWith('//')) u = 'https:' + u;
      else if (u.startsWith('/')) u = 'https://vkusvill.ru' + u;
      return u;
    }
    const itempropMatch = html.match(/<img[^>]+itemprop=["']image["'][^>]+src=["']([^"']+)["']/i)
      || html.match(/<img[^>]+src=["']([^"']+)["'][^>]+itemprop=["']image["']/i);
    if (itempropMatch && itempropMatch[1]) {
      let u = itempropMatch[1].trim();
      if (u.startsWith('//')) u = 'https:' + u;
      else if (u.startsWith('/')) u = 'https://vkusvill.ru' + u;
      return u;
    }
    return null;
  } catch (e) {
    return null;
  }
}

function createProductCard(item, options = {}) {
  const { showReason = false, showPrice = true } = options;
  const color = getBadgeColor(item.score);
  const priceStr = item.price != null && item.price > 0 ? `${Math.round(item.price)} ₽` : '';
  const weightStr = (item.weight || '').trim();
  const metaStr = priceStr ? (weightStr ? `${priceStr} · ${weightStr}` : priceStr) : weightStr;
  let reasonHtml = '';
  if (showReason && item.reason) {
    const priceInfo = item.price_diff_pct != null ? ` (${item.price_diff_pct > 0 ? '+' : ''}${Math.round(item.price_diff_pct)}% по цене)` : '';
    reasonHtml = `<div class="vv-card-reason">${escapeHtml(item.reason)}${priceInfo}</div>`;
  }
  const imgHtml = '<div class="vv-card-image vv-card-image--placeholder" data-vv-img-src="' + escapeHtml(item.url || '') + '"></div>';
  return `
    <a href="${escapeHtml(item.url)}" class="vv-product-card" target="_blank" rel="noopener">
      ${imgHtml}
      <div class="vv-card-body">
        <span class="vv-card-badge vv-rating-badge--${color}">${item.score}/100</span>
        <div class="vv-card-name">${escapeHtml(item.name)}</div>
        ${showPrice && metaStr ? `<div class="vv-card-meta">${escapeHtml(metaStr)}</div>` : ''}
        ${reasonHtml}
      </div>
    </a>`;
}

function createRecommendationsBlock(recommendations, cluster, clusterTops) {
  const wrap = document.createElement('div');
  wrap.className = 'vv-recommendations-block';
  wrap.setAttribute('data-vv-recommendations', '');

  let html = '';
  if (cluster?.cluster_name) {
    html += `<div class="vv-cluster-badge">${escapeHtml(cluster.cluster_name)}</div>`;
  }
  if (recommendations?.length > 0) {
    html += '<div class="vv-recommendations-header">Лучшие альтернативы</div>';
    html += '<div class="vv-recommendations-cards">';
    recommendations.slice(0, 6).forEach((rec) => {
      html += createProductCard(rec, { showReason: true, showPrice: true });
    });
    html += '</div>';
  }
  if (clusterTops?.length > 0) {
    html += '<div class="vv-cluster-tops-section">';
    html += '<div class="vv-cluster-tops-header">Топ в категории</div>';
    html += '<div class="vv-recommendations-cards">';
    clusterTops.slice(0, 6).forEach((p) => {
      html += createProductCard({ ...p, reason: '', price_diff_pct: null }, { showReason: false, showPrice: true });
    });
    html += '</div></div>';
  }
  if (!html) return null;
  wrap.innerHTML = html;
  loadCardImages(wrap);
  return wrap;
}

async function loadCardImages(block) {
  const placeholders = block.querySelectorAll('.vv-card-image--placeholder[data-vv-img-src]');
  const promises = [];
  placeholders.forEach((el) => {
    const url = el.getAttribute('data-vv-img-src');
    if (!url) return;
    promises.push(
      fetchProductImageUrl(url).then((imgUrl) => {
        if (imgUrl && el.isConnected) {
          el.removeAttribute('data-vv-img-src');
          el.classList.remove('vv-card-image--placeholder');
          el.style.backgroundImage = `url(${imgUrl})`;
        }
      })
    );
  });
  await Promise.allSettled(promises);
}

// Страница товара: полный блок
function createProductPageBlock(score, pros, cons, badges = {}) {
  const s = Math.round(Number(score)) || 0;
  const color = getBadgeColor(s);
  const prosText = (pros || '').trim() || '—';
  const consText = (cons || '').trim() || '—';
  const isBest = badges?.is_best;
  const isValue = badges?.is_value;
  const badgesHtml =
    isBest || isValue
      ? `<div class="vv-product-badges">
          ${isBest ? '<span class="vv-badge-pill vv-badge-pill--best">Лучший товар</span>' : ''}
          ${isValue ? '<span class="vv-badge-pill vv-badge-pill--value">Выгодный</span>' : ''}
        </div>`
      : '';

  const block = document.createElement('div');
  block.className = 'vv-product-block vv-product-block--' + color;
  block.setAttribute('data-vv-product-block', '');
  block.innerHTML = `
    <div class="vv-product-header">
      <span class="vv-rating-badge vv-rating-badge--${color} vv-rating-badge--page">${s}/100</span>
      <span class="vv-product-header-label">Оценка полезности</span>
      ${badgesHtml}
    </div>
    <div class="vv-product-details">
      <div class="vv-product-pros">
        <span class="vv-product-icon vv-product-icon--plus">${SVG_PLUS}</span>
        <div class="vv-product-text">
          <strong>Плюсы</strong>
          <p>${escapeHtml(prosText)}</p>
        </div>
      </div>
      <div class="vv-product-cons">
        <span class="vv-product-icon vv-product-icon--minus">${SVG_MINUS}</span>
        <div class="vv-product-text">
          <strong>Минусы</strong>
          <p>${escapeHtml(consText)}</p>
        </div>
      </div>
    </div>
  `;
  return block;
}

const CATALOG_LINK_SELECTORS = [
  '.ProductCard__link',
  'a.js-product-detail-link',
  'a[href*="/goods/"][href$=".html"]',
];

function findInsertPoint(card) {
  const price = card.querySelector('.ProductCard__price, .ProductCard__cost, [class*="Price"], [class*="price"]');
  if (price) return price;
  const footer = card.querySelector('.ProductCard__footer, .ProductCard__bottom, [class*="footer"], [class*="Footer"]');
  if (footer) return footer;
  const title = card.querySelector('.ProductCard__title, .ProductCard__name, [class*="ProductCard__title"]');
  if (title) return title;
  return card;
}

function collectCatalogItems() {
  const items = [];
  const seenLinks = new Set();
  for (const sel of CATALOG_LINK_SELECTORS) {
    document.querySelectorAll(sel).forEach((link) => {
      if (!link.href || !link.href.includes('/goods/') || !link.href.match(/\/goods\/[^/]+\.html/)) return;
      if (link.closest('[class*="Search"], [class*="search"], header, [class*="Header"]')) return;
      if (seenLinks.has(link)) return;
      seenLinks.add(link);

      const card = link.closest('.ProductCard, [class*="ProductCard"], [class*="GoodsCard"], [class*="goods-card"]') || link.parentElement;
      if (!card || card.hasAttribute(PROCESSED_ATTR)) return;

      const nameEl = link.querySelector('.ProductCard__title, .ProductCard__name, [class*="ProductCard__title"], [class*="ProductCard__name"]') || link;
      const name = (nameEl.textContent || '').trim();
      if (!name || name.length < 2) return;

      card.setAttribute(PROCESSED_ATTR, '1');
      const url = (link.href || '').split('?')[0] || '';
      items.push({ insertPoint: findInsertPoint(card), name, card, url });
    });
  }
  return items;
}

const PRODUCT_PAGE_SELECTORS = ['.Product__title', '.ProductHeader__title', 'h1[itemprop="name"]', '.ProductPage__title', '.Goods__title'];

function collectProductPageItems() {
  const items = [];
  const seen = new Set();
  const isProductPage = window.location.pathname.match(/\/goods\/[^/]+\.html/);
  const checkAndAdd = (el) => {
    if (seen.has(el) || el.closest(`[${PROCESSED_ATTR}]`)) return;
    if (el.closest('[data-vv-product-block]')) return;
    const name = (el.textContent || '').replace(/\u00A0|&nbsp;/g, ' ').trim();
    if (!name) return;
    seen.add(el);
    const container = el.closest('.ProductHeader, .Product, .ProductPage, [class*="Product"]') || el.parentElement;
    if (container?.querySelector?.('[data-vv-product-block]')) return;
    container?.setAttribute(PROCESSED_ATTR, '1');
    items.push({ titleEl: el, name });
    return isProductPage;
  };
  for (const sel of PRODUCT_PAGE_SELECTORS) {
    for (const el of document.querySelectorAll(sel)) {
      if (checkAndAdd(el) && isProductPage) return items;
    }
  }
  if (isProductPage) {
    for (const h1 of document.querySelectorAll('h1')) {
      if (seen.has(h1) || !h1.textContent.trim()) continue;
      if (checkAndAdd(h1)) return items;
    }
  }
  return items;
}

function findCatalogGrid(cards) {
  if (!cards || cards.length < 2) return null;
  let parent = cards[0].parentElement;
  if (!parent) return null;
  while (parent && parent !== document.body) {
    const childrenWithCards = Array.from(parent.children).filter((ch) =>
      cards.some((c) => ch.contains(c) || ch === c)
    );
    if (childrenWithCards.length >= 2) return { grid: parent };
    parent = parent.parentElement;
  }
  return null;
}

function addCatalogSort(catalogItems, ratings) {
  const cards = catalogItems.map((i) => i.card).filter(Boolean);
  const gridInfo = findCatalogGrid(cards);
  if (!gridInfo) return;
  const { grid } = gridInfo;
  if (grid.querySelector(`[${SORT_BTN_ATTR}]`)) return;

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'vv-sort-btn';
  btn.setAttribute(SORT_BTN_ATTR, '');
  btn.textContent = 'Сначала полезные';
  grid.insertAdjacentElement('beforebegin', btn);

  let sorted = false;
  const getScore = (el) => {
    const card = el.querySelector?.('[data-vv-score]') || (el.hasAttribute('data-vv-score') ? el : null);
    return card ? Number(card.getAttribute('data-vv-score')) : -1;
  };

  btn.addEventListener('click', () => {
    const items = Array.from(grid.children);
    if (sorted) {
      items.sort((a, b) => Number(a.getAttribute('data-vv-original-index') ?? 9999) - Number(b.getAttribute('data-vv-original-index') ?? 9999));
      items.forEach((el) => grid.appendChild(el));
      sorted = false;
      btn.textContent = 'Сначала полезные';
      return;
    }
    items.forEach((el, i) => {
      if (!el.hasAttribute('data-vv-original-index')) el.setAttribute('data-vv-original-index', String(i));
    });
    items.sort((a, b) => getScore(b) - getScore(a));
    items.forEach((el) => grid.appendChild(el));
    sorted = true;
    btn.textContent = 'По умолчанию';
  });
}

function collectCartItems() {
  const items = [];
  const cartSelectors = ['.CartItem', '.Cart__item', '.CartProduct', '[class*="CartItem"]', '[class*="cart-item"]', '[class*="CartProduct"]'];
  const seenItems = new Set();
  for (const sel of cartSelectors) {
    document.querySelectorAll(sel).forEach((item) => {
      if (seenItems.has(item) || item.hasAttribute(PROCESSED_ATTR)) return;
      if (!item.querySelector?.('a[href*="/goods/"], [class*="title"], [class*="name"]')) return;
      if (item.closest('[class*="Search"], [class*="search"], [class*="Header"], header')) return;
      seenItems.add(item);
      const nameEl = item.querySelector('.CartItem__title, .CartItem__name, .CartProduct__title, [class*="CartItem__title"], [class*="cart-item__name"], a[href*="/goods/"]');
      const link = item.querySelector('a[href*="/goods/"]');
      const name = nameEl ? nameEl.textContent.replace(/\u00A0|&nbsp;/g, ' ').trim() : '';
      const url = link?.href ? link.href.split('?')[0] : '';
      if (!name) return;
      item.setAttribute(PROCESSED_ATTR, '1');
      items.push({ insertPoint: nameEl || item.querySelector('[class*="Price"]') || item, name, url });
    });
  }
  if (window.location.pathname.match(/\/cart\/?/)) {
    const cartMain = document.querySelector('[class*="Cart"], [class*="cart"], [class*="Basket"], main');
    const scope = cartMain || document.body;
    scope.querySelectorAll('a[href*="/goods/"]').forEach((link) => {
      if (link.closest('.ProductCard, [class*="ProductCard"], .Product, .ProductHeader, [class*="Search"], [class*="search"], header')) return;
      const row = link.closest('tr, li, [class*="CartItem"], [class*="cart-item"], [class*="item"], [class*="Item"]');
      if (!row || row.hasAttribute(PROCESSED_ATTR) || row.querySelector?.('[data-vv-rating-badge]')) return;
      const name = link.textContent.replace(/\u00A0|&nbsp;/g, ' ').trim();
      const url = link.href ? link.href.split('?')[0] : '';
      if (!name || name.length < 3) return;
      row.setAttribute(PROCESSED_ATTR, '1');
      items.push({ insertPoint: link, name, url });
    });
  }
  return items;
}

async function processAll() {
  const catalogItems = collectCatalogItems();
  const productPageItems = collectProductPageItems();
  const cartItems = collectCartItems();

  const allNames = [
    ...catalogItems.map((i) => i.name),
    ...productPageItems.map((i) => i.name),
    ...cartItems.map((i) => i.name),
  ];
  const uniqueNames = [...new Set(allNames)];
  if (uniqueNames.length === 0) return;

  const productPageUrl = window.location.pathname.match(/\/goods\/[^/]+\.html/) ? (window.location.href || '').split('?')[0] : '';
  const allUrls = [
    ...catalogItems.map((i) => i.url),
    ...cartItems.map((i) => i.url),
    productPageUrl,
  ].filter(Boolean);
  const [ratings, badges] = await Promise.all([
    fetchRatingsBatch(uniqueNames),
    fetchBadgesBatch(allUrls),
  ]);

  catalogItems.forEach(({ insertPoint, name, card, url }) => {
    const r = ratings[name] || {};
    const b = url ? (badges[url] || {}) : {};
    const block = createCatalogBadge(r.score, r.pros, r.cons, b);
    insertPoint.insertAdjacentElement('beforebegin', block);
    const score = r.score != null ? Number(r.score) : -1;
    card.setAttribute('data-vv-score', String(score));
  });

  const catalogCards = catalogItems.filter((i) => !i.card?.closest?.('[class*="Cart"], [class*="cart"]'));
  addCatalogSort(catalogCards, ratings);

  productPageItems.forEach(async ({ titleEl, name }) => {
    if (document.querySelector('[data-vv-product-block]')) return;
    if (titleEl.nextElementSibling?.hasAttribute?.('data-vv-product-block')) return;
    if (titleEl.parentElement?.querySelector?.('[data-vv-product-block]')) return;
    const r = ratings[name];
    if (r && r.score != null) {
      const productBadges = productPageUrl ? (badges[productPageUrl] || {}) : {};
      const block = createProductPageBlock(r.score, r.pros, r.cons, productBadges);
      titleEl.insertAdjacentElement('afterend', block);
      const url = window.location.href?.split('?')[0] || '';
      const ext = await fetchProductExtended(url, name);
      if (ext && block.isConnected && !document.querySelector('[data-vv-recommendations]')) {
        const recBlock = createRecommendationsBlock(
          ext.recommendations,
          ext.cluster,
          ext.cluster_tops
        );
        if (recBlock) {
          const deliveryBlock = document.querySelector('.VV23_Product_Service, [class*="Product_Service"]');
          if (deliveryBlock) {
            deliveryBlock.insertAdjacentElement('afterend', recBlock);
          } else {
            block.insertAdjacentElement('afterend', recBlock);
          }
        }
      }
    }
  });

  cartItems.forEach(({ insertPoint, name, url }) => {
    const r = ratings[name] || {};
    const b = url ? (badges[url] || {}) : {};
    const block = createCatalogBadge(r.score, r.pros, r.cons, b);
    insertPoint.insertAdjacentElement('afterend', block);
  });
}

async function processCartItems() {
  const items = collectCartItems();
  if (items.length === 0) return;
  const names = [...new Set(items.map((i) => i.name))];
  const urls = items.map((i) => i.url).filter(Boolean);
  const [ratings, badges] = await Promise.all([fetchRatingsBatch(names), fetchBadgesBatch(urls)]);
  items.forEach(({ insertPoint, name, url }) => {
    const r = ratings[name] || {};
    const b = url ? (badges[url] || {}) : {};
    const block = createCatalogBadge(r.score, r.pros, r.cons, b);
    insertPoint.insertAdjacentElement('afterend', block);
  });
}

function observeNew() {
  const observer = new MutationObserver((mutations) => {
    let ok = false;
    for (const m of mutations) {
      for (const node of m.addedNodes) {
        if (node.nodeType !== 1) continue;
        if (node.querySelector?.('.ProductCard__link, a.js-product-detail-link, a[href*="/goods/"], .CartItem, [class*="CartItem"], [class*="cart-item"]') ||
            node.classList?.contains('ProductCard__link') || node.classList?.contains('CartItem')) {
          ok = true;
          break;
        }
      }
      if (ok) break;
    }
    if (ok) {
      setTimeout(processAll, 100);
      if (window.location.pathname.match(/\/cart\/?/)) setTimeout(processCartItems, 500);
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
}

function init() {
  processAll();
  observeNew();
  if (window.location.pathname.match(/\/cart\/?/)) {
    setTimeout(processCartItems, 800);
    setTimeout(processCartItems, 2000);
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
