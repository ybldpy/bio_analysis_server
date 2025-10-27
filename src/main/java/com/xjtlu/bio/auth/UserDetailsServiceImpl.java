package com.xjtlu.bio.auth;

import java.util.HashMap;
import java.util.List;

import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.entity.BioRole;
import com.xjtlu.bio.entity.BioRoleExample;
import com.xjtlu.bio.entity.BioUser;
import com.xjtlu.bio.mapper.BioRoleMapper;
import com.xjtlu.bio.service.UserService;

import jakarta.annotation.Resource;
import jakarta.annotation.Resources;


@Service
public class UserDetailsServiceImpl implements UserDetailsService{



    @Resource
    private UserService userService;
    @Resource
    private BioRoleMapper roleMapper;

    private HashMap<Integer, String> roleMap;
    private boolean needLoadRoleMap = true;




    private void loadRoleMap(){
        needLoadRoleMap = false;
        if(roleMap == null){
            roleMap = new HashMap<>();
        }

        BioRoleExample bioRoleExample = new BioRoleExample();
        List<BioRole> roles = roleMapper.selectByExample(bioRoleExample);
        roles.forEach((role)->{
            roleMap.put((int)(role.getRid()), role.getRoleName());
        });
    }



    
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // TODO Auto-generated method stub
        
        BioUser user = userService.getUserByName(username);
        if (user == null) {
            throw new UsernameNotFoundException("User not found");
        }

        if(needLoadRoleMap){
            loadRoleMap();
        }

        UserDetailImpl userDetails = new UserDetailImpl(user, roleMap.get((int)(user.getRoleId())));
        return userDetails;
    }



    
}
