package org.ssor.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProtocolBinder {

	Class<?> managerClass();
	// If a protocol needs responsible headers then it means it
	// has dedicated manager as well
	Class<?>[] headers() default {};
}
