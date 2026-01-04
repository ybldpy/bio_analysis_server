package com.xjtlu.bio.auth;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.entity.BioUser;

public class UserDetailImpl implements UserDetails {


    private BioUser user;
    private String userRoleNameCache;

    public UserDetailImpl(BioUser user, String userRoleNameCache) {
        // TODO Auto-generated constructor stub
        this.user = user;
        this.userRoleNameCache = userRoleNameCache;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        // TODO Auto-generated method stub
        ArrayList<GrantedAuthority> auths = new ArrayList<>();
        auths.add(new SimpleGrantedAuthority(userRoleNameCache));
        return auths;
    }

    @Override
    public String getPassword() {
        return user.getPassword();
    }
    
    @Override
    public String getUsername() {
        // TODO Auto-generated method stub
        return user.getName();
    }
    public Long getUid(){
        return user.getUid();
    }
    
}
