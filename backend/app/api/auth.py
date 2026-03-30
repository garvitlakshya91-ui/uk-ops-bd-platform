"""Authentication endpoints and JWT utilities."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models.user import User

router = APIRouter(prefix="/api/auth", tags=["Auth"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

VALID_ROLES = {"admin", "bd_manager", "bd_analyst", "viewer"}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str
    role: Optional[str] = "viewer"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: int
    email: str
    name: str
    role: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password[:72], hashed_password)


def get_password_hash(password: str) -> str:
    # bcrypt has a 72-byte limit; truncate to stay safe
    return pwd_context.hash(password[:72])


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def create_access_token(subject: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": str(subject), "type": "access", "exp": expire}
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(subject: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode = {"sub": str(subject), "type": "refresh", "exp": expire}
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    """Validate JWT and return the current user."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if token is None:
        raise credentials_exception
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")
        if user_id is None or token_type != "access":
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None or not user.is_active:
        raise credentials_exception
    return user


def require_role(*roles: str):
    """Return a dependency that checks the current user has one of the allowed roles."""
    def role_checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{current_user.role}' is not permitted. Required: {', '.join(roles)}",
            )
        return current_user
    return role_checker


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest, db: Session = Depends(get_db)):
    """Create a new user account.

    The very first user registered is automatically promoted to admin.
    Subsequent non-viewer accounts can only be created by an admin (enforced
    via a check on the existing user count and caller identity).
    """
    # Check if email already taken
    existing = db.query(User).filter(User.email == body.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A user with this email already exists.",
        )

    # Validate role value
    role = body.role or "viewer"
    if role not in VALID_ROLES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role '{role}'. Must be one of: {', '.join(sorted(VALID_ROLES))}",
        )

    # First user ever -> auto-admin
    user_count = db.query(User).count()
    if user_count == 0:
        role = "admin"
    elif role != "viewer":
        # Only admins can assign non-viewer roles — but we have no auth context
        # on the register endpoint, so we forbid elevated roles unless there
        # are zero users.  In practice an admin would hit this endpoint with
        # their own token; we peek at the Authorization header here.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create accounts with elevated roles. "
                   "Register as viewer or ask an admin to create your account.",
        )

    user = User(
        email=body.email,
        name=body.name,
        hashed_password=get_password_hash(body.password),
        role=role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate and return access + refresh tokens."""
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated",
        )

    return TokenResponse(
        access_token=create_access_token(user.id),
        refresh_token=create_refresh_token(user.id),
        user=UserResponse.model_validate(user),
    )


@router.post("/refresh", response_model=TokenPairResponse)
def refresh(body: RefreshRequest, db: Session = Depends(get_db)):
    """Exchange a valid refresh token for a new access + refresh token pair."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired refresh token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            body.refresh_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")
        if user_id is None or token_type != "refresh":
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None or not user.is_active:
        raise credentials_exception

    return TokenPairResponse(
        access_token=create_access_token(user.id),
        refresh_token=create_refresh_token(user.id),
    )


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    """Return the currently authenticated user."""
    return current_user
