# ImmoAnalyse Authentication

NextAuth.js integration with Supabase for secure user authentication.

## Setup

### 1. Install Dependencies

```bash
npm install next-auth @supabase/supabase-js
```

### 2. Environment Variables

Add to `.env.local`:

```env
# Supabase
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# NextAuth
NEXTAUTH_SECRET=your-secret-key-min-32-chars
NEXTAUTH_URL=http://localhost:3000
```

Generate `NEXTAUTH_SECRET`:
```bash
openssl rand -base64 32
```

### 3. Database Schema

Create these tables in Supabase:

**user_profiles table**:
```sql
CREATE TABLE user_profiles (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  email VARCHAR NOT NULL,
  first_name VARCHAR,
  last_name VARCHAR,
  avatar_url VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_user_profiles_user_id ON user_profiles(user_id);
```

## Files

- **`lib/auth.ts`** - NextAuth configuration with Supabase provider
- **`app/api/auth/[...nextauth]/route.ts`** - NextAuth API route
- **`app/api/auth/signup/route.ts`** - User registration endpoint
- **`app/(auth)/login/page.tsx`** - Login form
- **`app/(auth)/signup/page.tsx`** - Registration form

## Flow

### Login
1. User fills email + password form
2. Credentials sent to `/api/auth/[...nextauth]` route
3. NextAuth verifies with Supabase Auth
4. Session created (JWT-based)
5. Redirect to `/dashboard`

### Registration
1. User fills sign-up form
2. Request sent to `/api/auth/signup`
3. User created in Supabase Auth
4. Profile created in `user_profiles` table
5. Confirmation email sent (Supabase)
6. Redirect to login page

### Session Management
- Strategy: JWT (JSON Web Tokens)
- Max age: 30 days
- Token refreshed on page access
- Stored in secure HTTP-only cookies

## Usage in Components

### Check Authentication
```typescript
'use client';

import { useSession } from 'next-auth/react';

export default function MyComponent() {
  const { data: session, status } = useSession();

  if (status === 'loading') return <div>Loading...</div>;
  if (status === 'unauthenticated') return <div>Not logged in</div>;

  return <div>Welcome {session?.user?.name}</div>;
}
```

### Sign Out
```typescript
import { signOut } from 'next-auth/react';

<button onClick={() => signOut({ redirect: true, callbackUrl: '/' })}>
  Sign Out
</button>
```

### Protect Routes
```typescript
// middleware.ts
import { withAuth } from 'next-auth/middleware';

export const middleware = withAuth({
  pages: {
    signIn: '/auth/login',
  },
});

export const config = {
  matcher: ['/dashboard/:path*', '/generate/:path*'],
};
```

## API Routes

### `POST /api/auth/signup`

Register new user.

**Request**:
```json
{
  "email": "user@example.com",
  "password": "SecurePass123",
  "firstName": "John",
  "lastName": "Doe"
}
```

**Response (201)**:
```json
{
  "success": true,
  "message": "User created successfully. Check your email to confirm.",
  "user": {
    "id": "uuid",
    "email": "user@example.com"
  }
}
```

### `POST /api/auth/callback/credentials`

Login user (NextAuth endpoint).

**Request**:
```json
{
  "email": "user@example.com",
  "password": "SecurePass123"
}
```

## Development

### Test with Demo Credentials

Demo account (pre-created in Supabase):
- Email: `demo@example.com`
- Password: `demo123456`

### Password Requirements

- Minimum 8 characters
- At least 1 uppercase letter
- At least 1 number
- No special character requirement (but recommended)

### Error Handling

- Invalid credentials → "Invalid credentials"
- Email already exists → "Email already in use"
- Password mismatch → "Passwords do not match"
- Network error → "An error occurred"

## Production

Before deploying:

1. ✅ Set `NEXTAUTH_SECRET` to a strong random string
2. ✅ Set `NEXTAUTH_URL` to production domain
3. ✅ Enable email verification in Supabase
4. ✅ Setup SMTP for sending emails
5. ✅ Setup password reset flow (TODO)
6. ✅ Enable HTTPS only

## Next Steps

- [ ] Email verification flow
- [ ] Password reset flow
- [ ] Social login (Google, GitHub)
- [ ] Two-factor authentication
- [ ] Role-based access control
- [ ] Profile edit page

## Troubleshooting

**"NextAuth config not found"**
→ Ensure `NEXTAUTH_SECRET` is set in `.env.local`

**"Supabase connection failed"**
→ Check `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`

**"Session not persisting"**
→ Ensure cookies are enabled in browser
→ Check NEXTAUTH_URL matches your domain

**"Email already in use"**
→ That email is registered in Supabase
→ Use "Forgot password" or choose different email

## References

- [NextAuth.js Docs](https://next-auth.js.org)
- [Supabase Auth Docs](https://supabase.com/docs/guides/auth)
- [Next.js Middleware](https://nextjs.org/docs/app/building-your-application/routing/middleware)
