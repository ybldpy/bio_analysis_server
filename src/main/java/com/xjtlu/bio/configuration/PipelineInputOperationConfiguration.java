package com.xjtlu.bio.configuration;

import java.io.InputStream;
import java.lang.reflect.Method;

import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.Advisor;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.annotation.Role;
import org.springframework.core.Ordered;
import org.springframework.core.type.AnnotationMetadata;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Acquisition;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Operation;
import com.xjtlu.bio.service.PipelineInputService;
import com.xjtlu.bio.service.PipelineService;

/** Adds coordination around legacy entry points without changing their bodies. */
@Configuration(proxyBeanMethods = false)
@Import(PipelineInputOperationConfiguration.AutoProxyRegistrar.class)
public class PipelineInputOperationConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(PipelineInputOperationConfiguration.class);

    @Bean
    public PipelineInputOperationCoordinator pipelineInputOperationCoordinator() {
        return new PipelineInputOperationCoordinator();
    }

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public Advisor pipelineInputOperationAdvisor(PipelineInputOperationCoordinator coordinator) {
        StaticMethodMatcherPointcut pointcut = new StaticMethodMatcherPointcut() {
            @Override
            public boolean matches(Method method, Class<?> targetClass) {
                return isSingleUpload(method, targetClass) || isStart(method, targetClass);
            }
        };
        MethodInterceptor interceptor = invocation -> {
            boolean singleUpload = "createInputs".equals(invocation.getMethod().getName());
            long pipelineId = (Long) invocation.getArguments()[0];
            Operation operation = singleUpload ? Operation.SINGLE_UPLOAD : Operation.START;
            Acquisition acquisition = coordinator.tryAcquire(pipelineId, operation);
            if (!acquisition.acquired()) {
                if (singleUpload) {
                    // The legacy controller opens this stream before entering
                    // the service. Its normal storage owner is never called.
                    closeRejectedUpload((InputStream) invocation.getArguments()[4]);
                    if (acquisition.blockedBy() == Operation.START) {
                        return new Result<Long>(PipelineInputService.RESPONSE_CODE_PIPELINE_ALREADY_START,
                                -1L, "分析任务正在启动");
                    }
                    return new Result<Long>(Result.DUPLICATE_OPERATION, -1L, "分析任务数据正在整批上传，请稍后重试");
                }
                String message = acquisition.blockedBy() == Operation.START
                        ? "分析任务正在启动" : "分析任务数据正在上传，请稍后重试";
                return new Result<>(Result.DUPLICATE_OPERATION, null, message);
            }
            try (var permit = acquisition.permit()) {
                return invocation.proceed();
            }
        };
        DefaultPointcutAdvisor advisor = new DefaultPointcutAdvisor(pointcut, interceptor);
        // Keep the permit outside transactional advice, including commit and
        // rollback. Infrastructure role is required by the transaction APC.
        advisor.setOrder(Ordered.HIGHEST_PRECEDENCE + 50);
        return advisor;
    }

    private static boolean isSingleUpload(Method method, Class<?> targetClass) {
        Class<?>[] parameters = method.getParameterTypes();
        return PipelineInputService.class.isAssignableFrom(targetClass)
                && method.getName().equals("createInputs")
                && parameters.length == 5
                && parameters[0] == long.class
                && parameters[1] == String.class
                && parameters[2] == int.class
                && parameters[3] == String.class
                && parameters[4] == InputStream.class;
    }

    private static boolean isStart(Method method, Class<?> targetClass) {
        return PipelineService.class.isAssignableFrom(targetClass)
                && method.getName().equals("startPipeline")
                && method.getParameterCount() == 1
                && method.getParameterTypes()[0] == long.class;
    }

    private static void closeRejectedUpload(InputStream stream) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (Exception exception) {
            logger.warn("Could not close the rejected pipeline input stream", exception);
        }
    }

    /** Register the standard Spring APC without introducing an AspectJ dependency. */
    public static final class AutoProxyRegistrar implements ImportBeanDefinitionRegistrar {
        @Override
        public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
            AopConfigUtils.registerAutoProxyCreatorIfNecessary(registry);
        }
    }
}
