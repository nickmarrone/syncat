// syncat web UI — hand-written vanilla JS, no framework, no build step
// (SPEC.md §9). Hash-routed single page app that polls GET /api/status
// every 2s (there is no SSE endpoint in this build — see the README/
// SPEC.md §8 note about /api/events being deferred).
//
// XSS: every piece of data rendered here — peer names, share names, file
// paths, error strings — can originate from another machine and is fully
// attacker-controlled. This file never uses innerHTML/outerHTML with
// interpolated data anywhere; the `el()` helper below builds the DOM with
// document.createElement/textContent/Text nodes exclusively, and the only
// place a raw string becomes an *attribute* value is through
// Element.setAttribute with attribute names we choose ourselves (never
// attacker data), which cannot execute script. Search this file for
// "innerHTML" — it should not appear.

'use strict';

// --- tiny safe DOM builder ---------------------------------------------

/** Build an element. attrs values are set via setAttribute/className/
 * addEventListener — never innerHTML. children may be strings (wrapped
 * in a Text node, so never parsed as HTML), Nodes, or nested arrays. */
function el(tag, attrs, children) {
  const node = document.createElement(tag);
  attrs = attrs || {};
  for (const key of Object.keys(attrs)) {
    const value = attrs[key];
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on') && typeof value === 'function') node.addEventListener(key.slice(2), value);
    else if (value === true) node.setAttribute(key, '');
    else node.setAttribute(key, String(value));
  }
  appendChildren(node, children);
  return node;
}

function appendChildren(node, children) {
  if (children === undefined || children === null) return;
  const list = Array.isArray(children) ? children : [children];
  for (const child of list) {
    if (child === undefined || child === null) continue;
    if (Array.isArray(child)) {
      appendChildren(node, child);
    } else if (child instanceof Node) {
      node.append(child);
    } else {
      node.append(document.createTextNode(String(child)));
    }
  }
}

function text(t) {
  return document.createTextNode(String(t));
}

// --- app state -----------------------------------------------------------

const state = {
  token: null,
  status: null,
  statusError: null,
  pollTimer: null,
};

const trashState = { shareId: null, entries: [], error: null };

// Purely presentational state: which disclosures are open. It lives
// outside `state` because nothing here comes from (or goes back to) the
// daemon — it only has to survive the 2s poll re-render, which rebuilds
// the whole view from scratch.
const ui = {
  tokenExpanded: false,
  expandedPeers: new Set(),
};

// --- API client ------------------------------------------------------------

async function apiFetch(path, opts) {
  opts = opts || {};
  const headers = Object.assign({}, opts.headers, { 'X-Syncat-Token': state.token || '' });
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  const raw = await res.text();
  let data = null;
  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch (_) {
      // non-JSON body; leave data null
    }
  }
  if (!res.ok) {
    const message = (data && data.error && data.error.message) || res.statusText || ('HTTP ' + res.status);
    const err = new Error(message);
    err.status = res.status;
    throw err;
  }
  return data;
}

function apiGet(path) {
  return apiFetch(path);
}
function apiPost(path, body) {
  return apiFetch(path, { method: 'POST', body: JSON.stringify(body || {}) });
}
function apiPatch(path, body) {
  return apiFetch(path, { method: 'PATCH', body: JSON.stringify(body || {}) });
}
function apiDelete(path) {
  return apiFetch(path, { method: 'DELETE' });
}

async function fetchUIToken() {
  const res = await fetch('/ui-token');
  const raw = await res.text();
  let data = null;
  try {
    data = JSON.parse(raw);
  } catch (_) {
    // ignore
  }
  if (!res.ok || !data || !data.token) {
    const message = (data && data.error && data.error.message) || 'failed to fetch UI token';
    throw new Error(message);
  }
  return data.token;
}

// --- polling ---------------------------------------------------------------

async function pollStatus() {
  try {
    state.status = await apiGet('/api/status');
    state.statusError = null;
  } catch (err) {
    state.statusError = err.message || String(err);
  }
  render();
}

function startPolling() {
  stopPolling();
  pollStatus();
  state.pollTimer = setInterval(() => {
    if (document.hidden) return; // don't spin an idle background tab
    pollStatus();
  }, 2000);
}

function stopPolling() {
  if (state.pollTimer) clearInterval(state.pollTimer);
  state.pollTimer = null;
}

document.addEventListener('visibilitychange', () => {
  if (!document.hidden && state.token) pollStatus();
});

// --- formatting helpers ------------------------------------------------

function isZeroTime(iso) {
  return !iso || iso.indexOf('0001-01-01') === 0;
}

function formatTime(iso) {
  if (isZeroTime(iso)) return 'never';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function formatDuration(seconds) {
  seconds = Math.max(0, Math.floor(seconds || 0));
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (d > 0) return d + 'd ' + h + 'h';
  if (h > 0) return h + 'h ' + m + 'm';
  if (m > 0) return m + 'm ' + s + 's';
  return s + 's';
}

function formatBytes(n) {
  if (!n || n <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return (i === 0 ? v.toFixed(0) : v.toFixed(v >= 10 ? 0 : 1)) + ' ' + units[i];
}

const STATE_LABELS = {
  connected: 'Connected',
  connecting: 'Connecting…',
  backing_off: 'Backing off',
  disconnected: 'Disconnected',
};

function stateBadge(value) {
  const label = STATE_LABELS[value] || value || 'unknown';
  return el('span', { class: 'badge state-' + value }, [el('span', { class: 'dot' }), ' ' + label]);
}

const ACCESS_LABELS = {
  none: 'No access',
  pending: 'Pending',
  granted: 'Granted',
  denied: 'Denied',
  revoked: 'Revoked',
};

function accessBadge(value) {
  const label = ACCESS_LABELS[value] || value || 'unknown';
  return el('span', { class: 'badge access-' + value }, label);
}

function peerLabel(peerKey, peerName) {
  return peerName || (peerKey ? peerKey.slice(0, 12) + '…' : 'unknown peer');
}

// --- toasts ------------------------------------------------------------

function showToast(message, isError) {
  const stack = document.getElementById('toasts');
  const toast = el('div', { class: 'toast' + (isError ? ' error' : '') }, message);
  stack.append(toast);
  setTimeout(() => toast.remove(), isError ? 6000 : 3000);
}

// --- dialogs (confirm + subscribe) --------------------------------------

function confirmThen(message, onConfirm) {
  const dialog = el('dialog', { class: 'confirm-dialog' });
  dialog.append(el('p', {}, message));
  const cancelBtn = el('button', {}, 'Cancel');
  const okBtn = el('button', { class: 'danger' }, 'Confirm');
  dialog.append(el('div', { class: 'dialog-actions' }, [cancelBtn, okBtn]));
  document.body.append(dialog);
  dialog.addEventListener('close', () => dialog.remove());
  cancelBtn.addEventListener('click', () => dialog.close());
  okBtn.addEventListener('click', () => {
    dialog.close();
    onConfirm();
  });
  dialog.showModal();
}

function subscribeDialog(remoteShare) {
  const dialog = el('dialog', { class: 'confirm-dialog' });
  dialog.append(el('h2', {}, 'Subscribe to "' + remoteShare.name + '"'));
  dialog.append(el('p', { class: 'faint' }, 'from ' + peerLabel(remoteShare.peer_key, remoteShare.peer_name)));

  const pathInput = el('input', { type: 'text', placeholder: '/absolute/local/path' });
  const modeSelect = el('select', {}, [
    el('option', { value: 'mirror' }, 'Mirror (two-way sync)'),
    el('option', { value: 'receive-only' }, 'Receive-only'),
  ]);
  const errorP = el('p', { class: 'faint' }, '');

  dialog.append(el('div', { class: 'field' }, [el('label', {}, 'Local path'), pathInput]));
  dialog.append(el('div', { class: 'field' }, [el('label', {}, 'Mode'), modeSelect]));
  dialog.append(errorP);

  const cancelBtn = el('button', {}, 'Cancel');
  const okBtn = el('button', { class: 'primary' }, 'Subscribe');
  dialog.append(el('div', { class: 'dialog-actions' }, [cancelBtn, okBtn]));

  document.body.append(dialog);
  dialog.addEventListener('close', () => dialog.remove());
  cancelBtn.addEventListener('click', () => dialog.close());
  okBtn.addEventListener('click', async () => {
    const localPath = pathInput.value.trim();
    if (!localPath) {
      errorP.textContent = 'Local path is required.';
      return;
    }
    okBtn.disabled = true;
    try {
      await apiPost('/api/subscriptions', {
        peer: remoteShare.peer_key,
        share_id: remoteShare.share_id,
        local_path: localPath,
        mode: modeSelect.value,
      });
      showToast('Subscribed to ' + remoteShare.name);
      dialog.close();
      await pollStatus();
    } catch (err) {
      errorP.textContent = err.message;
      okBtn.disabled = false;
    }
  });
  dialog.showModal();
  pathInput.focus();
}

// --- Dashboard view ------------------------------------------------------

function renderNodeCard(status) {
  const card = el('div', { class: 'card' });
  card.append(el('h2', {}, 'This node'));

  const nameInput = el('input', { type: 'text', id: 'node-name-input' });
  nameInput.value = status.node_name;
  const saveBtn = el('button', { class: 'primary' }, 'Save');
  saveBtn.addEventListener('click', async () => {
    const name = nameInput.value.trim();
    if (!name || name === status.node_name) return;
    saveBtn.disabled = true;
    try {
      await apiPatch('/api/node', { name: name });
      showToast('Node renamed');
      await pollStatus();
    } catch (err) {
      showToast('Failed to rename node: ' + err.message, true);
    } finally {
      saveBtn.disabled = false;
    }
  });
  card.append(el('div', { class: 'field' }, [
    el('label', { for: 'node-name-input' }, 'Name'),
    el('div', { class: 'row' }, [nameInput, saveBtn]),
  ]));

  card.append(el('div', { class: 'field' }, [
    el('label', {}, "Node token — paste this into a peer's \"Add a peer\" form to connect"),
    renderTokenBox(status.node_token),
  ]));

  card.append(el('p', { class: 'faint' }, 'Short ID ' + status.short_id + ' · uptime ' + formatDuration(status.uptime_seconds)));
  return card;
}

// The node token is ~90 chars of base32 — long enough that showing it in
// full used to give the card its own horizontal scrollbar. Collapsed, we
// show the head of it and an ellipsis; expanded, it wraps over as many
// lines as it needs. Either way Copy copies the whole thing.
const TOKEN_PREVIEW_CHARS = 24;

function renderTokenBox(token) {
  token = token || '';
  const expanded = ui.tokenExpanded;
  const truncatable = token.length > TOKEN_PREVIEW_CHARS;
  const shown = expanded || !truncatable ? token : token.slice(0, TOKEN_PREVIEW_CHARS) + '…';

  const code = el('code', { class: expanded ? 'wrap' : null }, shown);

  const copyBtn = el('button', {}, 'Copy');
  copyBtn.addEventListener('click', () => copyToClipboard(token, copyBtn));

  const children = [code];
  if (truncatable) {
    const toggle = el('button', { class: 'link', 'data-focus-key': 'node-token', 'aria-expanded': expanded ? 'true' : 'false' }, expanded ? 'Hide' : 'Show full');
    toggle.addEventListener('click', () => {
      ui.tokenExpanded = !ui.tokenExpanded;
      render();
      refocus('button.link', 'node-token');
    });
    children.push(toggle);
  }
  children.push(copyBtn);
  return el('div', { class: 'token-box' + (expanded ? ' expanded' : '') }, children);
}

async function copyToClipboard(value, btn) {
  const original = btn.textContent;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(value);
    } else {
      const ta = document.createElement('textarea');
      ta.value = value;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.append(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    btn.textContent = 'Copied!';
  } catch (_) {
    btn.textContent = 'Copy failed';
  }
  setTimeout(() => {
    btn.textContent = original;
  }, 1500);
}

function renderAddPeerForm() {
  const card = el('div', { class: 'card' });
  card.append(el('h2', {}, 'Add a peer'));
  const tokenInput = el('input', { type: 'text', id: 'peer-token-input', placeholder: 'sc1:...', required: true });
  const nameInput = el('input', { type: 'text', id: 'peer-name-input', placeholder: '(optional)' });
  const submitBtn = el('button', { class: 'primary', type: 'submit' }, 'Add peer');
  const form = el('form', { class: 'inline-form' }, [
    el('div', { class: 'field grow' }, [el('label', { for: 'peer-token-input' }, "Peer's token"), tokenInput]),
    el('div', { class: 'field' }, [el('label', { for: 'peer-name-input' }, 'Display name'), nameInput]),
    submitBtn,
  ]);
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const token = tokenInput.value.trim();
    if (!token) return;
    submitBtn.disabled = true;
    try {
      await apiPost('/api/peers', { token: token, name: nameInput.value.trim() });
      showToast('Peer added');
      tokenInput.value = '';
      nameInput.value = '';
      await pollStatus();
    } catch (err) {
      showToast('Failed to add peer: ' + err.message, true);
    } finally {
      submitBtn.disabled = false;
    }
  });
  card.append(form);
  return card;
}

/** The shares this peer offers us, and the shares of ours they hold
 * access to — the two sides of the relationship, gathered once so the
 * collapsed row can count them and the expanded body can list them. */
function peerRelations(peer, status) {
  const offered = (status.remote_shares || []).filter((rs) => rs.peer_key === peer.peer_key);
  const subs = (status.subscriptions || []).filter((sub) => sub.peer_key === peer.peer_key);
  const usingOurs = [];
  for (const share of status.shares || []) {
    for (const a of share.access || []) {
      if (a.peer_key === peer.peer_key) usingOurs.push({ share: share, access: a.access });
    }
  }
  return { offered: offered, subs: subs, usingOurs: usingOurs };
}

/** render() replaces the whole view, so a disclosure control that just
 * got clicked loses focus with it. Re-find its replacement by a
 * data-attribute we set ourselves (never interpolating peer data into a
 * selector) and hand focus back, so keyboard use survives the rebuild. */
function refocus(selector, key) {
  const nodes = document.querySelectorAll(selector);
  for (const node of nodes) {
    if (node.dataset.focusKey === key) {
      node.focus();
      return;
    }
  }
}

function plural(n, word) {
  return n + ' ' + word + (n === 1 ? '' : 's');
}

/** One peer as a single row that expands in place. Collapsed it is a
 * name, a short id, a one-line summary and a connection badge; expanded
 * it adds the full key, the shares in both directions, and Remove. */
function renderPeerRow(peer, status) {
  const rel = peerRelations(peer, status);
  const expanded = ui.expandedPeers.has(peer.id);

  const summaryBits = [];
  if (rel.offered.length > 0) summaryBits.push(plural(rel.offered.length, 'share') + ' offered');
  if (rel.subs.length > 0) summaryBits.push(plural(rel.subs.length, 'subscription'));
  if (rel.usingOurs.length > 0) summaryBits.push(plural(rel.usingOurs.length, 'share') + ' of ours');

  const summary = el('button', { class: 'peer-summary', type: 'button', 'data-focus-key': peer.id, 'aria-expanded': expanded ? 'true' : 'false' }, [
    el('span', { class: 'chevron', 'aria-hidden': 'true' }, expanded ? '▾' : '▸'),
    el('span', { class: 'peer-name' }, peer.name || peer.remote_name || peer.short_id),
    el('span', { class: 'faint mono peer-id' }, peer.short_id),
    el('span', { class: 'faint peer-meta' }, summaryBits.join(' · ')),
    stateBadge(peer.state),
  ]);
  summary.addEventListener('click', () => {
    if (expanded) ui.expandedPeers.delete(peer.id);
    else ui.expandedPeers.add(peer.id);
    render();
    refocus('.peer-summary', peer.id);
  });

  const row = el('div', { class: 'card peer-row' + (expanded ? ' expanded' : '') }, summary);
  if (!expanded) {
    if (peer.last_error) row.append(el('p', { class: 'faint peer-error' }, 'Last error: ' + peer.last_error));
    return row;
  }
  row.append(renderPeerDetail(peer, rel));
  return row;
}

/** The expanded half of a peer row: everything that would be noise in a
 * list — the full key, connection history, both share directions, and
 * the destructive Remove, kept at the bottom where it can't be hit by
 * accident. */
function renderPeerDetail(peer, rel) {
  const body = el('div', { class: 'peer-detail' });

  body.append(el('p', { class: 'faint mono key' }, peer.peer_key));
  if (peer.last_error) {
    body.append(el('p', { class: 'faint' }, 'Last error: ' + peer.last_error));
  }
  if (peer.state === 'connected' && !isZeroTime(peer.connected_since)) {
    body.append(el('p', { class: 'faint' }, 'Connected since ' + formatTime(peer.connected_since)));
  } else if (!isZeroTime(peer.last_connected_at)) {
    body.append(el('p', { class: 'faint' }, 'Last connected ' + formatTime(peer.last_connected_at)));
  }

  body.append(el('h3', {}, 'Shares they offer'));
  if (rel.offered.length === 0) {
    body.append(el('p', { class: 'faint' }, 'None offered yet.'));
  } else {
    const list = el('ul', { class: 'plain' });
    for (const rs of rel.offered) {
      const sub = rel.subs.find((x) => x.share_id === rs.share_id);
      const right = sub
        ? el('span', { class: 'faint mono' }, sub.local_path)
        : (() => {
            const btn = el('button', {}, 'Subscribe');
            btn.addEventListener('click', () => subscribeDialog(rs));
            return btn;
          })();
      list.append(el('li', { class: 'list-item' }, [
        el('div', { class: 'stack' }, [
          el('span', {}, rs.name + ' (' + rs.permission + (rs.approval_required ? ', approval required' : '') + ')'),
          el('div', { class: 'row tight' }, [accessBadge(rs.access), sub ? el('span', { class: 'faint' }, 'subscribed · ' + sub.mode + (sub.paused ? ' · paused' : '')) : null]),
        ]),
        right,
      ]));
    }
    body.append(list);
  }

  body.append(el('h3', {}, 'Shares of ours they use'));
  if (rel.usingOurs.length === 0) {
    body.append(el('p', { class: 'faint' }, 'None.'));
  } else {
    const list = el('ul', { class: 'plain' });
    for (const u of rel.usingOurs) {
      list.append(el('li', { class: 'list-item' }, [el('span', {}, u.share.name), accessBadge(u.access)]));
    }
    body.append(list);
  }

  const removeBtn = el('button', { class: 'danger' }, 'Remove peer');
  removeBtn.addEventListener('click', () => {
    confirmThen('Remove peer "' + (peer.name || peer.short_id) + '"? This disconnects and forgets it.', async () => {
      try {
        await apiDelete('/api/peers/' + encodeURIComponent(peer.id));
        ui.expandedPeers.delete(peer.id);
        showToast('Peer removed');
        await pollStatus();
      } catch (err) {
        showToast('Failed to remove peer: ' + err.message, true);
      }
    });
  });
  body.append(el('div', { class: 'peer-actions' }, removeBtn));

  return body;
}

function renderRejectedCard(status) {
  const rejected = status.rejected_connections || [];
  if (rejected.length === 0) return null;
  const card = el('div', { class: 'card' });
  card.append(el('h2', {}, 'Rejected connection attempts'));
  const list = el('ul', { class: 'plain' });
  for (const r of rejected.slice(-10).reverse()) {
    list.append(el('li', {}, formatTime(r.at) + ' — ' + peerLabel(r.peer_key, r.peer_name) + ': ' + r.reason));
  }
  card.append(list);
  return card;
}

function renderDashboard(status) {
  const wrap = el('div');
  wrap.append(renderNodeCard(status));
  wrap.append(renderAddPeerForm());

  const peersSection = el('div', { class: 'section' });
  const peers = status.peers || [];
  peersSection.append(el('h2', {}, 'Peers' + (peers.length > 0 ? ' (' + peers.length + ')' : '')));
  if (peers.length === 0) {
    peersSection.append(el('div', { class: 'empty-state' }, "No peers yet — paste a peer's token above to connect."));
  } else {
    for (const peer of peers) peersSection.append(renderPeerRow(peer, status));
  }
  wrap.append(peersSection);

  const rejected = renderRejectedCard(status);
  if (rejected) wrap.append(rejected);
  return wrap;
}

// --- Shares view -----------------------------------------------------------

function renderAddShareForm() {
  const card = el('div', { class: 'card' });
  card.append(el('h2', {}, 'Add a share'));
  const pathInput = el('input', { type: 'text', id: 'share-path-input', placeholder: '/absolute/path', required: true });
  const nameInput = el('input', { type: 'text', id: 'share-name-input', placeholder: 'my-photos', required: true });
  const permSelect = el('select', { id: 'share-perm-input' }, [
    el('option', { value: 'read-only' }, 'Read-only'),
    el('option', { value: 'read-write' }, 'Read-write'),
  ]);
  const approvalCheckbox = el('input', { type: 'checkbox', id: 'share-approval-input' });
  const submitBtn = el('button', { class: 'primary', type: 'submit' }, 'Add share');
  const form = el('form', { class: 'inline-form' }, [
    el('div', { class: 'field' }, [el('label', { for: 'share-path-input' }, 'Local directory'), pathInput]),
    el('div', { class: 'field' }, [el('label', { for: 'share-name-input' }, 'Name'), nameInput]),
    el('div', { class: 'field' }, [el('label', { for: 'share-perm-input' }, 'Permission'), permSelect]),
    el('label', { class: 'checkbox-row' }, [approvalCheckbox, 'Require approval']),
    submitBtn,
  ]);
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const path = pathInput.value.trim();
    const name = nameInput.value.trim();
    if (!path || !name) return;
    submitBtn.disabled = true;
    try {
      await apiPost('/api/shares', {
        path: path,
        name: name,
        permission: permSelect.value,
        approval_required: approvalCheckbox.checked,
      });
      showToast('Share added');
      await pollStatus();
    } catch (err) {
      showToast('Failed to add share: ' + err.message, true);
    } finally {
      submitBtn.disabled = false;
    }
  });
  card.append(form);
  return card;
}

function renderShareAccessList(share) {
  const access = share.access || [];
  if (access.length === 0) {
    return el('p', { class: 'faint' }, 'No peer has requested access.');
  }
  const list = el('ul', { class: 'plain' });
  for (const a of access) {
    const btnRow = el('div', { class: 'row' });
    for (const choice of [['Grant', 'granted'], ['Deny', 'denied'], ['Revoke', 'revoked']]) {
      const label = choice[0];
      const value = choice[1];
      if (a.access === value) continue;
      const btn = el('button', {}, label);
      btn.addEventListener('click', async () => {
        try {
          const patch = { access: {} };
          patch.access[a.peer_key] = value;
          await apiPatch('/api/shares/' + encodeURIComponent(share.id), patch);
          showToast('Access updated');
          await pollStatus();
        } catch (err) {
          showToast('Failed to update access: ' + err.message, true);
        }
      });
      btnRow.append(btn);
    }
    list.append(el('li', { class: 'list-item' }, [
      el('div', { class: 'stack' }, [el('span', {}, peerLabel(a.peer_key, a.peer_name)), accessBadge(a.access)]),
      btnRow,
    ]));
  }
  return list;
}

function renderShareItem(share) {
  const card = el('div', { class: 'card' });

  const removeBtn = el('button', { class: 'danger' }, 'Remove');
  removeBtn.addEventListener('click', () => {
    confirmThen('Remove share "' + share.name + '"? Local files are left in place; peers lose access.', async () => {
      try {
        await apiDelete('/api/shares/' + encodeURIComponent(share.id));
        showToast('Share removed');
        await pollStatus();
      } catch (err) {
        showToast('Failed to remove share: ' + err.message, true);
      }
    });
  });
  card.append(el('div', { class: 'row between' }, [
    el('div', { class: 'stack' }, [el('span', { class: 'title' }, share.name), el('span', { class: 'faint mono' }, share.path)]),
    removeBtn,
  ]));

  const nameInput = el('input', { type: 'text' });
  nameInput.value = share.name;
  const nameSaveBtn = el('button', {}, 'Save');
  nameSaveBtn.addEventListener('click', async () => {
    const newName = nameInput.value.trim();
    if (!newName || newName === share.name) return;
    try {
      await apiPatch('/api/shares/' + encodeURIComponent(share.id), { name: newName });
      showToast('Share renamed');
      await pollStatus();
    } catch (err) {
      showToast('Failed to rename share: ' + err.message, true);
    }
  });

  const permSelect = el('select', {}, [
    el('option', { value: 'read-only' }, 'Read-only'),
    el('option', { value: 'read-write' }, 'Read-write'),
  ]);
  permSelect.value = share.permission;
  permSelect.addEventListener('change', async () => {
    try {
      await apiPatch('/api/shares/' + encodeURIComponent(share.id), { permission: permSelect.value });
      showToast('Permission updated');
      await pollStatus();
    } catch (err) {
      showToast('Failed to update permission: ' + err.message, true);
      permSelect.value = share.permission;
    }
  });

  const approvalCheckbox = el('input', { type: 'checkbox' });
  approvalCheckbox.checked = share.approval_required;
  approvalCheckbox.addEventListener('change', async () => {
    try {
      await apiPatch('/api/shares/' + encodeURIComponent(share.id), { approval_required: approvalCheckbox.checked });
      showToast('Approval setting updated');
      await pollStatus();
    } catch (err) {
      showToast('Failed to update approval setting: ' + err.message, true);
      approvalCheckbox.checked = share.approval_required;
    }
  });

  card.append(el('div', { class: 'row' }, [
    el('div', { class: 'field' }, [el('label', {}, 'Name'), el('div', { class: 'row' }, [nameInput, nameSaveBtn])]),
    el('div', { class: 'field' }, [el('label', {}, 'Permission'), permSelect]),
    el('label', { class: 'checkbox-row' }, [approvalCheckbox, 'Require approval']),
  ]));

  card.append(el('h3', {}, 'Peer access'));
  card.append(renderShareAccessList(share));
  return card;
}

function renderSubscriptionItem(sub) {
  const left = el('div', { class: 'stack' }, [
    el('span', { class: 'title' }, (sub.share_name || sub.share_id) + ' from ' + peerLabel(sub.peer_key, sub.peer_name)),
    el('span', { class: 'faint mono' }, sub.local_path),
    el('span', { class: 'faint' }, 'mode: ' + sub.mode + ' · ' + (sub.connected ? 'connected' : 'not connected') + ' · access: ' + sub.access),
  ]);
  for (const w of sub.warnings || []) {
    left.append(el('div', { class: 'warn-banner' }, (w.reverted ? 'Reverted: ' : 'Warning: ') + w.rel_path + ' — ' + w.reason));
  }

  const pauseCheckbox = el('input', { type: 'checkbox' });
  pauseCheckbox.checked = sub.paused;
  pauseCheckbox.addEventListener('change', async () => {
    try {
      await apiPatch('/api/subscriptions/' + encodeURIComponent(sub.id), { paused: pauseCheckbox.checked });
      showToast(pauseCheckbox.checked ? 'Subscription paused' : 'Subscription resumed');
      await pollStatus();
    } catch (err) {
      showToast('Failed to update subscription: ' + err.message, true);
      pauseCheckbox.checked = sub.paused;
    }
  });

  const removeBtn = el('button', { class: 'danger' }, 'Remove');
  removeBtn.addEventListener('click', () => {
    confirmThen('Remove subscription to "' + (sub.share_name || sub.share_id) + '"?', async () => {
      try {
        await apiDelete('/api/subscriptions/' + encodeURIComponent(sub.id));
        showToast('Subscription removed');
        await pollStatus();
      } catch (err) {
        showToast('Failed to remove subscription: ' + err.message, true);
      }
    });
  });

  return el('li', { class: 'list-item' }, [
    left,
    el('div', { class: 'row' }, [el('label', { class: 'checkbox-row' }, [pauseCheckbox, 'Paused']), removeBtn]),
  ]);
}

async function loadTrash(shareId) {
  trashState.shareId = shareId || null;
  trashState.error = null;
  if (!shareId) {
    trashState.entries = [];
    render();
    return;
  }
  try {
    const data = await apiGet('/api/shares/' + encodeURIComponent(shareId) + '/trash');
    trashState.entries = data.entries || [];
  } catch (err) {
    trashState.error = err.message;
    trashState.entries = [];
  }
  render();
}

function renderTrashSection(status) {
  const card = el('div', { class: 'card' });
  card.append(el('h2', {}, 'Trash'));
  const shares = status.shares || [];
  if (shares.length === 0) {
    card.append(el('p', { class: 'faint' }, 'Add a share to browse its trash.'));
    return card;
  }
  const select = el('select', {}, [el('option', { value: '' }, 'Select a share…')].concat(
    shares.map((s) => {
      const opt = el('option', { value: s.id }, s.name);
      if (trashState.shareId === s.id) opt.selected = true;
      return opt;
    })
  ));
  select.addEventListener('change', () => loadTrash(select.value));
  card.append(select);

  if (trashState.shareId) {
    if (trashState.error) {
      card.append(el('div', { class: 'error-banner' }, [el('span', {}, trashState.error)]));
    } else if (trashState.entries.length === 0) {
      card.append(el('p', { class: 'empty-state' }, 'Trash is empty.'));
    } else {
      const tbody = el('tbody');
      for (const entry of trashState.entries) {
        const restoreBtn = el('button', {}, 'Restore');
        restoreBtn.addEventListener('click', async () => {
          restoreBtn.disabled = true;
          try {
            await apiPost('/api/shares/' + encodeURIComponent(trashState.shareId) + '/trash/restore', { rel_path: entry.rel_path });
            showToast('Restored ' + entry.rel_path);
            await loadTrash(trashState.shareId);
          } catch (err) {
            showToast('Failed to restore: ' + err.message, true);
            restoreBtn.disabled = false;
          }
        });
        tbody.append(el('tr', {}, [
          el('td', { class: 'mono' }, entry.rel_path),
          el('td', {}, formatTime(entry.trashed_at)),
          el('td', {}, formatBytes(entry.size)),
          el('td', {}, restoreBtn),
        ]));
      }
      const table = el('table', { class: 'trash-table' }, [
        el('thead', {}, el('tr', {}, [el('th', {}, 'Path'), el('th', {}, 'Trashed at'), el('th', {}, 'Size'), el('th', {}, '')])),
        tbody,
      ]);
      card.append(el('div', { class: 'table-wrap' }, table));
    }
  }
  return card;
}

function renderShares(status) {
  const wrap = el('div');
  wrap.append(renderAddShareForm());

  const sharesSection = el('div', { class: 'section' });
  sharesSection.append(el('h2', {}, 'Your shares'));
  const shares = status.shares || [];
  if (shares.length === 0) {
    sharesSection.append(el('div', { class: 'empty-state' }, 'No shares yet — add a local directory above to start offering it.'));
  } else {
    for (const share of shares) sharesSection.append(renderShareItem(share));
  }
  wrap.append(sharesSection);

  const subsSection = el('div', { class: 'section' });
  subsSection.append(el('h2', {}, 'Your subscriptions'));
  const subs = status.subscriptions || [];
  if (subs.length === 0) {
    subsSection.append(el('div', { class: 'empty-state' }, 'You are not subscribed to any peer shares yet — expand a peer on the dashboard to subscribe to what they offer.'));
  } else {
    const list = el('ul', { class: 'plain' });
    for (const sub of subs) list.append(renderSubscriptionItem(sub));
    subsSection.append(el('div', { class: 'card' }, list));
  }
  wrap.append(subsSection);

  wrap.append(renderTrashSection(status));
  return wrap;
}

// --- routing / top-level render ------------------------------------------

function currentRoute() {
  const hash = location.hash.replace(/^#/, '');
  // '/peers' folded into the dashboard; keep old bookmarks working.
  if (!hash || hash === '/peers') return '/';
  return hash;
}

function updateNavHighlight() {
  const route = currentRoute();
  document.querySelectorAll('#tabs a').forEach((a) => {
    a.classList.toggle('active', a.dataset.route === route);
  });
}

// The version comes from the daemon's own /api/status, not from anything
// baked into these assets, so what the header shows is the build actually
// serving the page — which is the point of showing it at all.
function updateVersion() {
  const slot = document.getElementById('version');
  if (!slot) return;
  const version = state.status && state.status.version;
  slot.textContent = version ? 'v' + version : '';
  slot.title = version ? 'daemon version ' + version : '';
}

function updateConnIndicator() {
  const indicator = document.getElementById('conn-indicator');
  indicator.classList.remove('error');
  if (state.statusError) {
    indicator.textContent = 'daemon unreachable';
    indicator.classList.add('error');
  } else if (state.status) {
    indicator.textContent = 'connected · uptime ' + formatDuration(state.status.uptime_seconds);
  } else {
    indicator.textContent = '';
  }
}

function renderFatal(message) {
  const app = document.getElementById('app');
  app.replaceChildren(el('div', { class: 'error-banner' }, [el('span', {}, message)]));
}

function render() {
  updateNavHighlight();
  updateVersion();
  updateConnIndicator();
  const app = document.getElementById('app');

  if (!state.status) {
    app.replaceChildren(el('p', { class: 'muted' }, state.statusError ? 'Waiting for the syncat daemon…' : 'Loading…'));
    return;
  }

  const route = currentRoute();

  // Don't yank the DOM out from under someone mid-edit on a live poll
  // tick: if focus is inside #app on the same route we're about to
  // re-render, skip this rebuild. The next tick (after they blur, e.g.
  // by clicking Save/Cancel) or the next route change will catch up.
  const active = document.activeElement;
  const isEditing = active && app.contains(active) && (active.tagName === 'INPUT' || active.tagName === 'SELECT' || active.tagName === 'TEXTAREA');
  if (isEditing && app.dataset.route === route) {
    return;
  }

  const frag = document.createDocumentFragment();
  if (state.statusError) {
    frag.append(el('div', { class: 'error-banner' }, [el('span', {}, 'Lost connection to the daemon: ' + state.statusError)]));
  }
  if (route === '/shares') frag.append(renderShares(state.status));
  else frag.append(renderDashboard(state.status));

  app.replaceChildren(frag);
  app.dataset.route = route;
}

// --- boot ------------------------------------------------------------------

async function boot() {
  try {
    state.token = await fetchUIToken();
  } catch (err) {
    renderFatal('Could not reach the syncat daemon: ' + err.message);
    setTimeout(boot, 2000);
    return;
  }
  window.addEventListener('hashchange', render);
  startPolling();
}

boot();
