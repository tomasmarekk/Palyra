import {
  Button,
  Checkbox,
  Description,
  Form,
  Input,
  Label,
  ListBox,
  Select,
  Switch,
  TextArea,
  TextField
} from "@heroui/react";
import type { FormEvent, HTMLAttributes, ReactNode } from "react";

import { joinClassNames } from "./utils";

type TextInputFieldProps = {
  label: ReactNode;
  value: string;
  onChange: (value: string) => void;
  description?: ReactNode;
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  readOnly?: boolean;
  type?: "text" | "password" | "search" | "url" | "number" | "email";
  autoComplete?: string;
  name?: string;
};

type TextAreaFieldProps = Omit<TextInputFieldProps, "type"> & {
  rows?: number;
};

type SelectFieldOption = {
  key: string;
  label: ReactNode;
  description?: ReactNode;
};

type SelectFieldProps = {
  label: ReactNode;
  value: string;
  onChange: (value: string) => void;
  options: readonly SelectFieldOption[];
  description?: ReactNode;
  disabled?: boolean;
  placeholder?: string;
  name?: string;
};

type ToggleFieldProps = {
  label: ReactNode;
  description?: ReactNode;
  checked: boolean;
  onChange: (checked: boolean) => void;
  disabled?: boolean;
};

type AppFormProps = {
  className?: string;
  onSubmit?: (event: FormEvent<HTMLFormElement>) => void;
  children: ReactNode;
};

type ActionClusterProps = HTMLAttributes<HTMLDivElement> & {
  children: ReactNode;
};

export function AppForm({ className, onSubmit, children }: AppFormProps) {
  return (
    <Form className={joinClassNames("workspace-form", className)} onSubmit={onSubmit}>
      {children}
    </Form>
  );
}

export function TextInputField({
  label,
  value,
  onChange,
  description,
  placeholder,
  required = false,
  disabled = false,
  readOnly = false,
  type = "text",
  autoComplete,
  name
}: TextInputFieldProps) {
  return (
    <TextField
      className="workspace-field"
      isDisabled={disabled}
      isRequired={required}
      name={name}
    >
      <Label>{label}</Label>
      <Input
        autoComplete={autoComplete}
        placeholder={placeholder}
        readOnly={readOnly}
        type={type}
        value={value}
        onChange={(event) => onChange(event.currentTarget.value)}
      />
      {description !== undefined && <Description>{description}</Description>}
    </TextField>
  );
}

export function TextAreaField({
  label,
  value,
  onChange,
  description,
  placeholder,
  required = false,
  disabled = false,
  readOnly = false,
  autoComplete,
  name,
  rows = 4
}: TextAreaFieldProps) {
  return (
    <TextField
      className="workspace-field"
      isDisabled={disabled}
      isRequired={required}
      name={name}
    >
      <Label>{label}</Label>
      <TextArea
        autoComplete={autoComplete}
        placeholder={placeholder}
        readOnly={readOnly}
        rows={rows}
        value={value}
        onChange={(event) => onChange(event.currentTarget.value)}
      />
      {description !== undefined && <Description>{description}</Description>}
    </TextField>
  );
}

export function SelectField({
  label,
  value,
  onChange,
  options,
  description,
  disabled = false,
  placeholder,
  name
}: SelectFieldProps) {
  const selectedValue = value.trim().length === 0 ? null : value;

  return (
    <Select
      className="workspace-field"
      isDisabled={disabled}
      name={name}
      placeholder={placeholder}
      value={selectedValue}
      onChange={(nextValue) => onChange(nextValue === null ? "" : String(nextValue))}
    >
      <Label>{label}</Label>
      <Select.Trigger>
        <Select.Value />
        <Select.Indicator />
      </Select.Trigger>
      {description !== undefined && <Description>{description}</Description>}
      <Select.Popover>
        <ListBox>
          {options.map((option) => (
            <ListBox.Item
              id={option.key}
              key={option.key}
              textValue={stringLabel(option.label)}
            >
              <div className="workspace-listbox-option">
                <Label>{option.label}</Label>
                {option.description !== undefined ? (
                  <Description>{option.description}</Description>
                ) : null}
                <ListBox.ItemIndicator />
              </div>
            </ListBox.Item>
          ))}
        </ListBox>
      </Select.Popover>
    </Select>
  );
}

export function CheckboxField({
  label,
  description,
  checked,
  onChange,
  disabled = false
}: ToggleFieldProps) {
  return (
    <Checkbox
      className="workspace-toggle-field"
      isDisabled={disabled}
      isSelected={checked}
      onChange={onChange}
    >
      <Checkbox.Control>
        <Checkbox.Indicator />
      </Checkbox.Control>
      <Checkbox.Content>
        <div className="workspace-toggle-field__content">
          <Label>{label}</Label>
          {description !== undefined ? <Description>{description}</Description> : null}
        </div>
      </Checkbox.Content>
    </Checkbox>
  );
}

export function SwitchField({
  label,
  description,
  checked,
  onChange,
  disabled = false
}: ToggleFieldProps) {
  return (
    <Switch
      className="workspace-toggle-field"
      isDisabled={disabled}
      isSelected={checked}
      onChange={onChange}
    >
      <Switch.Control>
        <Switch.Thumb />
      </Switch.Control>
      <Switch.Content>
        <div className="workspace-toggle-field__content">
          <Label>{label}</Label>
          {description !== undefined ? <Description>{description}</Description> : null}
        </div>
      </Switch.Content>
    </Switch>
  );
}

export function ActionCluster({ children, className, ...props }: ActionClusterProps) {
  return (
    <div className={joinClassNames("workspace-inline-actions", className)} {...props}>
      {children}
    </div>
  );
}

export function ActionButton({
  children,
  ...props
}: React.ComponentProps<typeof Button>) {
  return <Button {...props}>{children}</Button>;
}

function stringLabel(value: ReactNode): string | undefined {
  return typeof value === "string" ? value : undefined;
}
