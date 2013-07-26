package com.emar.recsys.user.util;

public interface IUserException {
	// AD 
	public static final int ErrADNull = 1;
	public static final int ErrADItemNull = 10;
	
	// Media
	public static final int ErrUrlNull = 100;
	public static final int ErrUrlMalForm = 110;
	
	// User
	public static final int ErrUserNull = 1000;
	public static final int ErrUserIndex = 1001;
	
	// LOG parse
	public static final int ErrLogNull = 10000;
	
}
