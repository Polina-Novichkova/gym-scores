
require("dotenv").config();

const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { Pool } = require("pg");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = Number(process.env.PORT || 3000);

const ROLES = ["DA1", "DA2", "DB1", "DB2", "E1", "E2", "A1", "A2"];

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSL === "true"
    ? { rejectUnauthorized: false }
    : undefined
});

app.use(express.static(path.join(__dirname, "public")));

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, database: "connected" });
  } catch (error) {
    console.error("Ошибка проверки БД:", error.message);
    res.status(503).json({
      ok: false,
      database: "unavailable"
    });
  }
});

let performances = [];
let currentIndex = 0;

const latestScores = new Map();

/* =========================
   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
========================= */

function value(g, ...keys) {
  for (const key of keys) {
    if (
      g[key] !== undefined &&
      g[key] !== null &&
      String(g[key]).trim() !== ""
    ) {
      return String(g[key]).trim();
    }
  }

  return "";
}

function startMinutes(text) {
  const match = String(text || "").match(
    /(\d{1,2}):(\d{2})\s*-/
  );

  return match
    ? Number(match[1]) * 60 + Number(match[2])
    : 99999;
}

/* =========================
   ПОДГОТОВКА ВЫСТУПЛЕНИЙ
========================= */

function normalizePerformances(parsed) {
  if (!Array.isArray(parsed)) {
    throw new Error("gymnasts.json должен содержать массив");
  }

  const people = parsed.map((g, index) => {
    const streamText =
      value(g, "Поток / Время", "Поток") ||
      `Поток ${g.Поток || ""}`;

    const streamMatch = streamText.match(
      /Поток\s*(\d+)/i
    );

    const streamNumber = Number(
      g.Поток ||
      g.streamNumber ||
      (streamMatch ? streamMatch[1] : 0)
    );

    const streamKey = streamNumber
      ? `Поток ${streamNumber}`
      : (streamText.includes("ОФП") ? "ОФП" : streamText);

    const timeText =
      streamText.match(
        /\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/
      )?.[0] ||
      value(g, "Время потока", "streamTime");

    return {
      sourceId: Number(g.id) || index + 1,

      number:
        Number(value(g, "Номер в потоке", "number")) ||
        index + 1,

      streamNumber,
      stream: streamKey,
      streamText,
      streamTime: timeText,
      streamOrder: startMinutes(streamText),

      name:
        value(g, "ФИО Гимнастки", "name") ||
        "Неизвестная участница",

      birthYear: value(g, "Год рождения", "birthYear"),
      club: value(g, "Город / Клуб", "club"),
      group: value(g, "Группа / Категория", "group"),

      // Сохраняем пустые места, чтобы номера видов не сдвигались.
      apparatus: [
        value(g, "Вид 1", "apparatus1"),
        value(g, "Вид 2", "apparatus2")
      ]
    };
  });

  const result = [];
  let nextId = 1;

  const sortedPeople = people.sort(
    (a, b) =>
      a.streamOrder - b.streamOrder ||
      a.number - b.number ||
      a.sourceId - b.sourceId
  );

  for (const p of sortedPeople) {
    const stages = p.apparatus
      .map((apparatus, index) => ({
        apparatus,
        stageIndex: index + 1
      }))
      .filter(stage => stage.apparatus);

    const actualStages = stages.length
      ? stages
      : [{
          apparatus: p.stream === "ОФП"
            ? "ОФП"
            : "Вид не указан",
          stageIndex: 1
        }];

    actualStages.forEach(({ apparatus, stageIndex }) => {
      result.push({
        id: nextId++,
        sourceId: p.sourceId,
        number: p.number,

        streamNumber: p.streamNumber,
        stream: p.stream,
        streamText: p.streamText,
        streamTime: p.streamTime,
        streamOrder: p.streamOrder,

        name: p.name,
        birthYear: p.birthYear,
        club: p.club,
        group: p.group,

        apparatus,
        stageIndex,
        stageCount: actualStages.length
      });
    });
  }

  result.sort(
    (a, b) =>
      a.streamOrder - b.streamOrder ||
      a.stageIndex - b.stageIndex ||
      a.number - b.number ||
      a.sourceId - b.sourceId
  );

  return result;
}

/* =========================
   ИНИЦИАЛИЗАЦИЯ
========================= */

async function initialize() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS performance_scores (
      performance_id INTEGER NOT NULL,
      judge_role TEXT NOT NULL CHECK (
        judge_role = ANY(
          ARRAY['DA1','DA2','DB1','DB2','E1','E2','A1','A2']
        )
      ),
      score NUMERIC(8,3) NOT NULL
        CHECK (score >= 0 AND score <= 100),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (performance_id, judge_role)
    )
  `);

  const jsonPath = path.join(
    __dirname,
    "public",
    "gymnasts.json"
  );

  if (!fs.existsSync(jsonPath)) {
    throw new Error(
      `Не найден файл расписания: ${jsonPath}`
    );
  }

  performances = normalizePerformances(
    JSON.parse(fs.readFileSync(jsonPath, "utf8"))
  );

  if (!performances.length) {
    throw new Error("В gymnasts.json нет участниц");
  }

  const result = await pool.query(
    "SELECT performance_id, judge_role, score FROM performance_scores"
  );

  latestScores.clear();

  for (const row of result.rows) {
    latestScores.set(
      `${row.performance_id}_${row.judge_role}`,
      Number(row.score)
    );
  }

  console.log(
    `Загружено выступлений: ${performances.length}; оценок новой системы: ${result.rowCount}`
  );
}

/* =========================
   ОТПРАВКА ДАННЫХ КЛИЕНТУ
========================= */

function sendInit(socket) {
  socket.emit("init_data", {
    gymnasts: performances,
    currentIndex,
    scores: Object.fromEntries(latestScores)
  });
}

/* =========================
   SOCKET.IO
========================= */

io.on("connection", socket => {
  console.log("Подключено устройство:", socket.id);

  sendInit(socket);

  // Общее переключение текущей гимнастки.
  socket.on("set_gymnast_index", (rawIndex, ack) => {
    const index = Number(rawIndex);

    if (
      !Number.isInteger(index) ||
      index < 0 ||
      index >= performances.length
    ) {
      if (typeof ack === "function") {
        ack({
          ok: false,
          error: "Неверный номер выступления"
        });
      }

      return;
    }

    currentIndex = index;

    io.emit("gymnast_changed", { currentIndex });

    if (typeof ack === "function") {
      ack({ ok: true });
    }
  });

  // Сохранение оценки судьи.
  socket.on("submit_score", async (data, ack) => {
    try {
      const performanceId = Number(data?.gymnastId);
      const judgeRole = String(data?.judgeRole || "");

      const scoreText = String(data?.score ?? "")
        .trim()
        .replace(",", ".");

      const score = Number(scoreText);

      if (!performances.some(p => p.id === performanceId)) {
        throw new Error("Неизвестное выступление");
      }

      if (!ROLES.includes(judgeRole)) {
        throw new Error("Неизвестная роль судьи");
      }

      if (
        !Number.isFinite(score) ||
        score < 0 ||
        score > 100 ||
        !/^\d+(?:\.\d{1,3})?$/.test(scoreText)
      ) {
        throw new Error(
          "Оценка должна быть от 0 до 100, максимум 3 знака после точки"
        );
      }

      const saved = await pool.query(
        `INSERT INTO performance_scores
          (performance_id, judge_role, score, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (performance_id, judge_role)
         DO UPDATE SET
           score = EXCLUDED.score,
           updated_at = NOW()
         RETURNING score, updated_at`,
        [performanceId, judgeRole, score]
      );

      const normalized = Number(saved.rows[0].score);

      latestScores.set(
        `${performanceId}_${judgeRole}`,
        normalized
      );

      const payload = {
        gymnastId: performanceId,
        judgeRole,
        score: normalized,
        updatedAt: saved.rows[0].updated_at
      };

      io.emit("score_updated", payload);

      if (typeof ack === "function") {
        ack({
          ok: true,
          score: normalized
        });
      }
    } catch (error) {
      console.error("Ошибка сохранения оценки:", error.message);

      if (typeof ack === "function") {
        ack({
          ok: false,
          error: error.message ||
            "Не удалось сохранить оценку"
        });
      }
    }
  });

  socket.on("disconnect", reason => {
    console.log(
      "Отключено устройство:",
      socket.id,
      reason
    );
  });
});

/* =========================
   ЗАПУСК СЕРВЕРА
========================= */

async function start() {
  try {
    await initialize();

    server.listen(PORT, "0.0.0.0", () => {
      console.log(
        `Gym Scores server запущен на порту ${PORT}`
      );
    });
  } catch (error) {
    console.error("Ошибка запуска:", error);

    await pool.end().catch(() => {});
    process.exit(1);
  }
}

/* =========================
   КОРРЕКТНОЕ ЗАВЕРШЕНИЕ
========================= */

let shuttingDown = false;

async function shutdown() {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log("Завершение сервера...");

  server.close(async () => {
    try {
      await pool.end();
    } finally {
      process.exit(0);
    }
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

start();
