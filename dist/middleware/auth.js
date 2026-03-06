import { validateSessionToken } from "../services/authService.js";
export async function requireUser(req, res, next) {
    try {
        const sessionToken = req.header("x-session-token")?.trim();
        if (!sessionToken) {
            res.status(401).json({ error: "Missing x-session-token header" });
            return;
        }
        const user = await validateSessionToken(sessionToken);
        if (!user) {
            res.status(401).json({ error: "Session expired or invalid" });
            return;
        }
        req.user = {
            userId: user.userId,
            email: user.email,
            role: user.role
        };
        next();
    }
    catch (error) {
        next(error);
    }
}
export function requireRole(allowed) {
    return (req, res, next) => {
        const role = req.user?.role;
        if (!role || !allowed.includes(role)) {
            res.status(403).json({ error: "Insufficient permissions" });
            return;
        }
        next();
    };
}
