// Deletes the signed-in user's account. Their records are removed by the
// database's ON DELETE CASCADE.
import { admin, cors, reply, requireUser } from "../_shared/http.ts";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return reply({ code: "method_not_allowed" }, 405);
  const user = await requireUser(req);
  if (!user) return reply({ code: "unauthorized" }, 401);
  const { error } = await admin().auth.admin.deleteUser(user.id);
  if (error) return reply({ code: "delete_failed", message: error.message }, 500);
  return reply({ deleted: true });
});
