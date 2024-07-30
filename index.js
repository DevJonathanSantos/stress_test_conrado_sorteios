const Joi = require("joi");
const { Pool } = require("pg");
const Hapi = require("hapi");
const Inert = require("inert");
const Vision = require("vision");
const HapiSwagger = require("hapi-swagger");
const port = process.env.PORT || 3000;
const server = new Hapi.Server({
  port,
});

const failAction = async (request, h, err) => {
  console.error("err", err);
  throw err;
};

const pool = new Pool({
  connectionString: `postgres://${process.env.POSTGRES_HOST}/${
    process.env.POSTGRES_DB || "heroes"
  }`,
  ssl: process.env.POSTGRES_SSL === "true",
});

const createTable = async () => {
  const query = `
CREATE TABLE tenants (
    tenant_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE tickets (
    ticket_id SERIAL PRIMARY KEY,
    code VARCHAR(20)
);
ALTER TABLE tickets ADD CONSTRAINT unique_code UNIQUE (code);

CREATE TABLE prize_draw (
    prize_draw_id SERIAL PRIMARY KEY,
    tenant_id INT REFERENCES tenants(tenant_id),
    ticket_id INT REFERENCES tickets(ticket_id),
    user_id INT REFERENCES users(user_id)
);


INSERT INTO tenants (name) VALUES ('Tenant A');
INSERT INTO tenants (name) VALUES ('Tenant B');

INSERT INTO users (name) VALUES ('User 1');
INSERT INTO users (name) VALUES ('User 2');


CREATE OR REPLACE FUNCTION generate_random_code() RETURNS TEXT AS $$
DECLARE
    code TEXT;
BEGIN
    -- Gerar um código aleatório de 10 caracteres
    code := substring(md5(random()::text) FROM 1 FOR 10);
    RETURN code;
END;
$$ LANGUAGE plpgsql;



DO $$
DECLARE
    num_tickets INT := 1000000; -- Defina o número de tickets que você deseja gerar
    i INT;
    random_code TEXT;
BEGIN
    FOR i IN 1..num_tickets LOOP
        LOOP
            random_code := generate_random_code();
            -- Tente inserir o ticket com o código gerado
            BEGIN
                INSERT INTO tickets (code) VALUES (random_code);
                EXIT; -- Saia do loop se a inserção for bem-sucedida
            EXCEPTION WHEN unique_violation THEN
                -- Continue tentando se ocorrer uma violação de chave única
                CONTINUE;
            END;
        END LOOP;
    END LOOP;
END;
$$;



CREATE OR REPLACE FUNCTION insert_prize_draw(p_tenant_id INT, p_user_id INT, p_num_tickets INT)
RETURNS VOID AS $$
BEGIN
  -- Selecione e bloqueie as linhas para garantir exclusividade
  WITH selected_tickets AS (
    SELECT t.ticket_id
    FROM tickets t
    WHERE t.ticket_id NOT IN (
      SELECT s.ticket_id
      FROM prize_draw s
      WHERE s.tenant_id = p_tenant_id
    )
    LIMIT p_num_tickets
    FOR UPDATE
  )
  -- Insira as linhas selecionadas na tabela prize_draw
  INSERT INTO prize_draw (tenant_id, ticket_id, user_id)
  SELECT p_tenant_id, ticket_id, p_user_id
  FROM selected_tickets;
EXCEPTION
  -- Em caso de erro, levantar exceção para que a transação seja revertida automaticamente
  WHEN OTHERS THEN
    RAISE;
END;
$$ LANGUAGE plpgsql;

  `;
  await pool.query(query);
};

const test = async () => {
  try {
    for (let index = 0; index < 500000; index++) {
      await pool.query("SELECT insert_prize_draw(1, 1, 2);");
    }
  } catch (err) {
    console.error(err.stack);
    res.status(500).json({ error: "Database error" });
  }
};

(async () => {
  if (!process.env.POSTGRES_HOST) {
    throw Error(
      "process.env.POSTGRES_HOST must be a: user:pass@ipService:port"
    );
  }
  await createTable();
  console.log("Postgres is running");

  await server.register([
    Inert,
    Vision,
    {
      plugin: HapiSwagger,
      options: {
        info: {
          title: "Node.js with Postgres Example - Erick Wendel",
          version: "1.0",
        },
      },
    },
  ]);

  server.route([
    {
      method: "POST",
      path: "/test",
      config: {
        handler: async (req) => {
          return await test();
        },
        description: "Create a hero",
        notes: "create a hero",
        tags: ["api"],
        // validate: {
        //   failAction,
        //   payload: {
        //     name: Joi.string().required(),
        //     power: Joi.string().required(),
        //   },
        // },
      },
    },
  ]);

  await server.start();
  console.log("Server running at", server.info.port);
})();
