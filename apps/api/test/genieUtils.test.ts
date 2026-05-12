import assert from 'node:assert/strict';
import test from 'node:test';

import {
  asRecord,
  genieMessageError,
  normalizeGenieStreamAttachments,
  normalizeQueryResult,
  normalizeStatementResponse,
  sqlValue,
  textValue,
  toGenieStreamMessage,
} from '../src/services/genieUtils.js';

// ---------------------------------------------------------------------------
// asRecord
// ---------------------------------------------------------------------------

test('asRecord returns the object when given a plain object', () => {
  const obj = { a: 1 };
  assert.equal(asRecord(obj), obj);
});

test('asRecord returns empty object for non-objects', () => {
  assert.deepEqual(asRecord(null), {});
  assert.deepEqual(asRecord(undefined), {});
  assert.deepEqual(asRecord(42), {});
  assert.deepEqual(asRecord('hello'), {});
  assert.deepEqual(asRecord([1, 2]), {});
});

// ---------------------------------------------------------------------------
// textValue
// ---------------------------------------------------------------------------

test('textValue extracts trimmed strings', () => {
  assert.equal(textValue('hello'), 'hello');
  assert.equal(textValue('  spaced  '), 'spaced');
  assert.equal(textValue(''), null);
  assert.equal(textValue('   '), null);
});

test('textValue returns null for null and undefined', () => {
  assert.equal(textValue(null), null);
  assert.equal(textValue(undefined), null);
});

test('textValue joins array elements', () => {
  assert.equal(textValue(['hello', 'world']), 'helloworld');
  assert.equal(textValue(['hello ', 'world']), 'helloworld');
  assert.equal(textValue([]), null);
  assert.equal(textValue(['', '  ']), null);
});

test('textValue extracts from nested content/text/value/markdown keys', () => {
  assert.equal(textValue({ content: 'from content' }), 'from content');
  assert.equal(textValue({ text: 'from text' }), 'from text');
  assert.equal(textValue({ value: 'from value' }), 'from value');
  assert.equal(textValue({ markdown: 'from markdown' }), 'from markdown');
});

test('textValue returns null for empty objects', () => {
  assert.equal(textValue({}), null);
});

// ---------------------------------------------------------------------------
// sqlValue
// ---------------------------------------------------------------------------

test('sqlValue extracts SQL from query/sql/statement/content keys', () => {
  assert.equal(sqlValue({ query: 'SELECT 1' }), 'SELECT 1');
  assert.equal(sqlValue({ sql: 'SELECT 2' }), 'SELECT 2');
  assert.equal(sqlValue({ statement: 'SELECT 3' }), 'SELECT 3');
  assert.equal(sqlValue({ content: 'SELECT 4' }), 'SELECT 4');
});

test('sqlValue returns null for empty or non-object', () => {
  assert.equal(sqlValue(null), null);
  assert.equal(sqlValue({}), null);
});

// ---------------------------------------------------------------------------
// genieMessageError
// ---------------------------------------------------------------------------

test('genieMessageError returns undefined for no error', () => {
  assert.equal(genieMessageError({}), undefined);
  assert.equal(genieMessageError({ error: null }), undefined);
});

test('genieMessageError returns string error directly', () => {
  assert.equal(genieMessageError({ error: 'something broke' }), 'something broke');
});

test('genieMessageError extracts message from error object', () => {
  assert.equal(genieMessageError({ error: { message: 'bad request' } }), 'bad request');
  assert.equal(genieMessageError({ error: { error_code: 'NOT_FOUND' } }), 'NOT_FOUND');
});

// ---------------------------------------------------------------------------
// normalizeQueryResult
// ---------------------------------------------------------------------------

test('normalizeQueryResult returns null for empty input', () => {
  assert.equal(normalizeQueryResult({}), null);
  assert.equal(normalizeQueryResult(null), null);
});

test('normalizeQueryResult extracts columns and rows', () => {
  const body = {
    statement_response: {
      manifest: {
        schema: {
          columns: [{ name: 'id' }, { name: 'cost' }],
        },
      },
      result: {
        data_array: [
          ['1', '100'],
          ['2', '200'],
        ],
      },
    },
  };
  const result = normalizeQueryResult(body);
  assert.deepEqual(result, {
    columns: ['id', 'cost'],
    rows: [
      ['1', '100'],
      ['2', '200'],
    ],
  });
});

test('normalizeQueryResult generates fallback column names', () => {
  const body = {
    statement_response: {
      manifest: {
        schema: {
          columns: [{ name: 'id' }, {}],
        },
      },
      result: {
        data_array: [['1', '2']],
      },
    },
  };
  const result = normalizeQueryResult(body);
  assert.deepEqual(result?.columns, ['id', 'column_2']);
});

test('normalizeQueryResult limits rows to 25', () => {
  const rows = Array.from({ length: 30 }, (_, i) => [String(i)]);
  const body = {
    statement_response: {
      manifest: { schema: { columns: [{ name: 'n' }] } },
      result: { data_array: rows },
    },
  };
  const result = normalizeQueryResult(body);
  assert.equal(result?.rows.length, 25);
});

// ---------------------------------------------------------------------------
// normalizeStatementResponse
// ---------------------------------------------------------------------------

test('normalizeStatementResponse returns null for empty input', () => {
  assert.equal(normalizeStatementResponse({}), null);
  assert.equal(normalizeStatementResponse(null), null);
});

test('normalizeStatementResponse extracts typed columns and rows', () => {
  const body = {
    manifest: {
      schema: {
        columns: [
          { name: 'id', type_name: 'INT' },
          { name: 'cost', type_name: 'DOUBLE' },
        ],
      },
    },
    result: {
      data_array: [
        [1, 100.5],
        [2, null],
      ],
    },
  };
  const result = normalizeStatementResponse(body);
  assert.deepEqual(result, {
    manifest: {
      schema: {
        columns: [
          { name: 'id', type_name: 'INT' },
          { name: 'cost', type_name: 'DOUBLE' },
        ],
      },
    },
    result: {
      data_array: [
        ['1', '100.5'],
        ['2', null],
      ],
    },
  });
});

// ---------------------------------------------------------------------------
// toGenieStreamMessage
// ---------------------------------------------------------------------------

test('toGenieStreamMessage maps API response fields to stream message', () => {
  const msg = toGenieStreamMessage({
    message_id: 'msg-1',
    conversation_id: 'conv-1',
    space_id: 'space-1',
    status: 'COMPLETED',
    content: 'Hello',
    attachments: [],
  });
  assert.equal(msg.messageId, 'msg-1');
  assert.equal(msg.conversationId, 'conv-1');
  assert.equal(msg.spaceId, 'space-1');
  assert.equal(msg.status, 'COMPLETED');
  assert.equal(msg.content, 'Hello');
  assert.deepEqual(msg.attachments, []);
  assert.equal(msg.error, undefined);
});

test('toGenieStreamMessage uses fallback values', () => {
  const msg = toGenieStreamMessage(
    {},
    { conversationId: 'fb-conv', messageId: 'fb-msg', spaceId: 'fb-space' },
  );
  assert.equal(msg.messageId, 'fb-msg');
  assert.equal(msg.conversationId, 'fb-conv');
  assert.equal(msg.spaceId, 'fb-space');
  assert.equal(msg.status, 'COMPLETED');
});

test('toGenieStreamMessage includes error from message', () => {
  const msg = toGenieStreamMessage({ error: 'oops' });
  assert.equal(msg.error, 'oops');
});

// ---------------------------------------------------------------------------
// normalizeGenieStreamAttachments
// ---------------------------------------------------------------------------

test('normalizeGenieStreamAttachments normalizes query attachments', () => {
  const attachments = normalizeGenieStreamAttachments([
    {
      attachment_id: 'att-1',
      query: { query: 'SELECT 1', title: 'Test query', statement_id: 'stmt-1' },
    },
  ]);
  assert.equal(attachments.length, 1);
  assert.equal(attachments[0].attachmentId, 'att-1');
  assert.equal(attachments[0].query?.query, 'SELECT 1');
  assert.equal(attachments[0].query?.title, 'Test query');
  assert.equal(attachments[0].query?.statementId, 'stmt-1');
});

test('normalizeGenieStreamAttachments normalizes text attachments', () => {
  const attachments = normalizeGenieStreamAttachments([{ id: 'att-2', text: 'Some answer' }]);
  assert.equal(attachments.length, 1);
  assert.equal(attachments[0].attachmentId, 'att-2');
  assert.equal(attachments[0].text?.content, 'Some answer');
  assert.equal(attachments[0].query, undefined);
});

test('normalizeGenieStreamAttachments handles empty array', () => {
  assert.deepEqual(normalizeGenieStreamAttachments([]), []);
});
